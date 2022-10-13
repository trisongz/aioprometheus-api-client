"""A Class for collection of metrics from a Prometheus Host."""
import os
import bz2
import json
import anyio
import httpx
import numpy
import logging
from dataclasses import dataclass
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from .exceptions import PrometheusApiClientException

# set up logging

_LOGGER = logging.getLogger(__name__)

# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 3
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]

PROM_API_URL = os.getenv('PROM_API_URL', 'http://127.0.0.1:9090')
PROM_MAX_RETRIES = int(os.getenv('PROM_MAX_RETRIES', 3))
PROM_DISABLE_SSL = os.getenv('PROM_DISABLE_SSL', 'false').lower() in {'true', '1'}


@dataclass
class SavedQuery:
    """A Class for custom query object."""

    query: str # The query to execute
    params: Optional[Dict[str, Any]] = None
    value: Optional[Any] = None


class PrometheusConnect:
    """
    A Class for collection of metrics from a Prometheus Host.

    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    :param disable_ssl: (bool) If set to True, will disable ssl certificate verification
        for the http requests made to the prometheus host
    :param retry: (Retry) Retry adapter to retry on HTTP errors
    """

    def __init__(
        self,
        url: str = PROM_API_URL,
        headers: Optional[Dict[str, Any]] = None,
        disable_ssl: bool = PROM_DISABLE_SSL,
        retries: int = PROM_MAX_RETRIES,
    ):
        """Functions as a Constructor for the class PrometheusConnect."""
        if url is None: raise TypeError("missing url")
        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None
        self.ssl_verification = not disable_ssl
        self._saved_queries: Dict[str, SavedQuery] = {}

        # Async Session
        self._asession = httpx.AsyncClient(
            base_url = self.url,
            headers = self.headers,
            transport = httpx.AsyncHTTPTransport(retries = retries),
            verify = self.ssl_verification,
        )

        # Sync Session
        self._session = httpx.Client(
            base_url = self.url,
            headers = self.headers,
            transport = httpx.HTTPTransport(retries = retries),
            verify = self.ssl_verification,
        )

    def check_prometheus_connection(self, params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Check Promethus connection.

        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._session.get(
            url = '/', params = params
        )
        return not response.is_error
    
    async def async_check_prometheus_connection(self, params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Check Promethus connection.

        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = await self._asession.get(
            url = '/', params = params
        )
        return not response.is_error

    def all_metrics(self, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Get the list of all the metrics that the prometheus host scrapes.

        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names of all the metrics available from the
            specified prometheus host
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        self._all_metrics = self.get_label_values(label_name="__name__", params=params)
        return self._all_metrics
    
    async def async_all_metrics(self, params: dict = None) -> List[Any]:
        """
        Get the list of all the metrics that the prometheus host scrapes.

        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names of all the metrics available from the
            specified prometheus host
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        self._all_metrics = await self.async_get_label_values(label_name="__name__", params=params)
        return self._all_metrics

    def get_label_values(self, label_name: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Get a list of all values for the label.

        :param label_name: (str) The name of the label for which you want to get all the values.
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names for the label from the specified prometheus host
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        response = self._session.get(
            url = f'/api/v1/label/{label_name}/values',
            params = params
        )
        if response.status_code == 200:
            labels = response.json()["data"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return labels
    
    async def async_get_label_values(self, label_name: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Get a list of all values for the label.

        :param label_name: (str) The name of the label for which you want to get all the values.
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names for the label from the specified prometheus host
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        response = await self._asession.get(
            url = f'/api/v1/label/{label_name}/values',
            params = params
        )
        if response.status_code == 200:
            labels = response.json()["data"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return labels

    def get_current_metric_value(
        self, metric_name: str, label_config: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        r"""
        Get the current metric value for the specified metric and label configuration.

        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "time"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code

        Example Usage:
          .. code-block:: python

              prom = PrometheusConnect()

              my_label_config = {'cluster': 'my_cluster_id', 'label_2': 'label_2_value'}

              prom.get_current_metric_value(metric_name='up', label_config=my_label_config)
        """
        params = params or {}
        data = []
        if label_config:
            label_list = [str(f"{key}=" + "'" + label_config[key] + "'") for key in label_config]

            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        # using the query API to get raw data
        response = self._session.get(
            url = '/api/v1/query',
            params = {**{"query": query}, **params},
        )

        if response.status_code == 200:
            data += response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return data
    
    async def async_get_current_metric_value(
        self, metric_name: str, label_config: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        r"""
        Get the current metric value for the specified metric and label configuration.

        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "time"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code

        Example Usage:
          .. code-block:: python

              prom = PrometheusConnect()

              my_label_config = {'cluster': 'my_cluster_id', 'label_2': 'label_2_value'}

              prom.get_current_metric_value(metric_name='up', label_config=my_label_config)
        """
        params = params or {}
        data = []
        if label_config:
            label_list = [str(f"{key}=" + "'" + label_config[key] + "'") for key in label_config]
            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        # using the query API to get raw data
        response = await self._asession.get(
            url = '/api/v1/query',
            params = {**{"query": query}, **params},
        )

        if response.status_code == 200:
            data += response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return data

    def get_metric_range_data(
        self,
        metric_name: str,
        label_config: Optional[Dict[str, Any]] = None,
        start_time: datetime = (datetime.now() - timedelta(minutes=10)),
        end_time: datetime = datetime.now(),
        chunk_size: timedelta = None,
        store_locally: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        r"""
        Get the current metric value for the specified metric and label configuration.

        :param metric_name: (str) The name of the metric.
        :param label_config: (dict) A dictionary specifying metric labels and their
            values.
        :param start_time:  (datetime) A datetime object that specifies the metric range start time.
        :param end_time: (datetime) A datetime object that specifies the metric range end time.
        :param chunk_size: (timedelta) Duration of metric data downloaded in one request. For
            example, setting it to timedelta(hours=3) will download 3 hours worth of data in each
            request made to the prometheus host
        :param store_locally: (bool) If set to True, will store data locally at,
            `"./metrics/hostname/metric_date/name_time.json.bz2"`
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :return: (list) A list of metric data for the specified metric in the given time
            range
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code

        """
        params = params or {}
        data = []

        _LOGGER.debug("start_time: %s", start_time)
        _LOGGER.debug("end_time: %s", end_time)
        _LOGGER.debug("chunk_size: %s", chunk_size)

        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError("start_time and end_time can only be of type datetime.datetime")

        if not chunk_size:
            chunk_size = end_time - start_time
        if not isinstance(chunk_size, timedelta):
            raise TypeError("chunk_size can only be of type datetime.timedelta")

        start = round(start_time.timestamp())
        end = round(end_time.timestamp())

        if end_time < start_time:
            raise ValueError("end_time must not be before start_time")

        if (end_time - start_time).total_seconds() < chunk_size.total_seconds():
            raise ValueError("specified chunk_size is too big")
        chunk_seconds = round(chunk_size.total_seconds())

        if label_config:
            label_list = [str(f"{key}=" + "'" + label_config[key] + "'") for key in label_config]

            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name
        _LOGGER.debug("Prometheus Query: %s", query)

        while start < end:
            if start + chunk_seconds > end:
                chunk_seconds = end - start

            # using the query API to get raw data
            response = self._session.get(
                url = '/api/v1/query_range',
                params = {
                    **{
                        "query": query,
                        "start": start,
                        "end": start + chunk_seconds,
                        "step": chunk_seconds,
                    },
                    **params,
                },
            )

            if response.status_code == 200:
                data += response.json()["data"]["result"]
            else:
                raise PrometheusApiClientException(
                    f"HTTP Status Code {response.status_code} ({response.content})"
                )
            if store_locally:
                # store it locally
                self._store_metric_values_local(
                    metric_name,
                    json.dumps(response.json()["data"]["result"]),
                    start + chunk_seconds,
                )

            start += chunk_seconds
        return data

    async def async_get_metric_range_data(
        self,
        metric_name: str,
        label_config: Optional[Dict[str, Any]] = None,
        start_time: datetime = (datetime.now() - timedelta(minutes=10)),
        end_time: datetime = datetime.now(),
        chunk_size: timedelta = None,
        store_locally: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        r"""
        Get the current metric value for the specified metric and label configuration.

        :param metric_name: (str) The name of the metric.
        :param label_config: (dict) A dictionary specifying metric labels and their
            values.
        :param start_time:  (datetime) A datetime object that specifies the metric range start time.
        :param end_time: (datetime) A datetime object that specifies the metric range end time.
        :param chunk_size: (timedelta) Duration of metric data downloaded in one request. For
            example, setting it to timedelta(hours=3) will download 3 hours worth of data in each
            request made to the prometheus host
        :param store_locally: (bool) If set to True, will store data locally at,
            `"./metrics/hostname/metric_date/name_time.json.bz2"`
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :return: (list) A list of metric data for the specified metric in the given time
            range
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code

        """
        params = params or {}
        data = []

        _LOGGER.debug("start_time: %s", start_time)
        _LOGGER.debug("end_time: %s", end_time)
        _LOGGER.debug("chunk_size: %s", chunk_size)

        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError("start_time and end_time can only be of type datetime.datetime")

        if not chunk_size:
            chunk_size = end_time - start_time
        if not isinstance(chunk_size, timedelta):
            raise TypeError("chunk_size can only be of type datetime.timedelta")

        start = round(start_time.timestamp())
        end = round(end_time.timestamp())

        if end_time < start_time:
            raise ValueError("end_time must not be before start_time")

        if (end_time - start_time).total_seconds() < chunk_size.total_seconds():
            raise ValueError("specified chunk_size is too big")
        chunk_seconds = round(chunk_size.total_seconds())

        if label_config:
            label_list = [str(f"{key}=" + "'" + label_config[key] + "'") for key in label_config]

            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name
        _LOGGER.debug("Prometheus Query: %s", query)

        while start < end:
            if start + chunk_seconds > end:
                chunk_seconds = end - start

            # using the query API to get raw data
            response = await self._asession.get(
                url = '/api/v1/query_range',
                params = {
                    **{
                        "query": query,
                        "start": start,
                        "end": start + chunk_seconds,
                        "step": chunk_seconds,
                    },
                    **params,
                },
            )

            if response.status_code == 200:
                data += response.json()["data"]["result"]
            else:
                raise PrometheusApiClientException(
                    f"HTTP Status Code {response.status_code} ({response.content})"
                )
            if store_locally:
                # store it locally
                await self._async_store_metric_values_local(
                    metric_name,
                    json.dumps(response.json()["data"]["result"]),
                    start + chunk_seconds,
                )

            start += chunk_seconds
        return data

    def _store_metric_values_local(self, metric_name: str, values: str, end_timestamp: int, compressed: bool = False) -> str:
        r"""
        Store metrics on the local filesystem, optionally  with bz2 compression.

        :param metric_name: (str) the name of the metric being saved
        :param values: (str) metric data in JSON string format
        :param end_timestamp: (int) timestamp in any format understood by \
            datetime.datetime.fromtimestamp()
        :param compressed: (bool) whether or not to apply bz2 compression
        :returns: (str) path to the saved metric file
        """
        if not values:
            _LOGGER.debug("No values for %s", metric_name)
            return None

        file_path = self._metric_filename(metric_name, end_timestamp)

        if compressed:
            payload = bz2.compress(values.encode("utf-8"))
            file_path = f"{file_path}.bz2"
        else:
            payload = values.encode("utf-8")

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(payload)

        return file_path
    
    async def _async_store_metric_values_local(self, metric_name: str, values: str, end_timestamp: int, compressed: bool = False) -> str:
        r"""
        Store metrics on the local filesystem, optionally  with bz2 compression.

        :param metric_name: (str) the name of the metric being saved
        :param values: (str) metric data in JSON string format
        :param end_timestamp: (int) timestamp in any format understood by \
            datetime.datetime.fromtimestamp()
        :param compressed: (bool) whether or not to apply bz2 compression
        :returns: (str) path to the saved metric file
        """
        if not values:
            _LOGGER.debug("No values for %s", metric_name)
            return None

        file_path = self._metric_filename(metric_name, end_timestamp)

        if compressed:
            payload = bz2.compress(str(values).encode("utf-8"))
            file_path = f"{file_path}.bz2"
        else:
            payload = str(values).encode("utf-8")

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        async with await anyio.open_file(file_path, 'wb') as file:
            await file.write(payload)
        return file_path

    def _metric_filename(self, metric_name: str, end_timestamp: int) -> str:
        r"""
        Add a timestamp to the filename before it is stored.

        :param metric_name: (str) the name of the metric being saved
        :param end_timestamp: (int) timestamp in any format understood by \
            datetime.datetime.fromtimestamp()
        :returns: (str) the generated path
        """
        end_time_stamp = datetime.fromtimestamp(end_timestamp)
        directory_name = end_time_stamp.strftime("%Y%m%d")
        timestamp = end_time_stamp.strftime("%Y%m%d%H%M")
        return f"./metrics/{self.prometheus_host}/{metric_name}/{directory_name}/{timestamp}.json"

    def custom_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        # sourcery skip: remove-unnecessary-cast
        """
        Send a custom query to a Prometheus Host.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.

        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = None
        # using the query API to get raw data
        response = self._session.get(
            url = '/api/v1/query',
            params={**{"query": str(query)}, **params}
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )

        return data
    
    async def async_custom_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        # sourcery skip: remove-unnecessary-cast
        """
        Send a custom query to a Prometheus Host.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.

        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = None
        # using the query API to get raw data
        response = await self._asession.get(
            url = '/api/v1/query',
            params={**{"query": str(query)}, **params}
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )

        return data

    def custom_query_range(
        self, query: str, start_time: datetime, end_time: datetime, step: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # sourcery skip: remove-unnecessary-cast
        """
        Send a query_range to a Prometheus Host.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.

        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "timeout"
        :returns: (dict) A dict of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        params = params or {}
        data = None
        # using the query_range API to get raw data
        response = self._session.get(
            url = '/api/v1/query_range',
            params={**{"query": str(query), "start": start, "end": end, "step": step}, **params}
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return data
    
    async def async_custom_query_range(
        self, query: str, start_time: datetime, end_time: datetime, step: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # sourcery skip: remove-unnecessary-cast
        """
        Send a query_range to a Prometheus Host.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.

        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "timeout"
        :returns: (dict) A dict of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        params = params or {}
        data = None
        # using the query_range API to get raw data
        response = await self._asession.get(
            url = '/api/v1/query_range',
            params={**{"query": str(query), "start": start, "end": end, "step": step}, **params}
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise PrometheusApiClientException(
                f"HTTP Status Code {response.status_code} ({response.content})"
            )
        return data

    def get_metric_aggregation(
        self,
        query: str,
        operations: List[str],
        start_time: datetime = None,
        end_time: datetime = None,
        step: str = "15",
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get aggregations on metric values received from PromQL query.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query. And, a
        list of operations to perform such as- sum, max, min, deviation, etc.
        with start_time, end_time and step.

        The received query is passed to the custom_query_range method which returns
        the result of the query and the values are extracted from the result.

        :param query: (str) This is a PromQL query, a few examples can be found
          at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param operations: (list) A list of operations to perform on the values.
          Operations are specified in string type.
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
          sent along with the API request, such as "timeout"
          Available operations - sum, max, min, variance, nth percentile, deviation
          and average.

        :returns: (dict) A dict of aggregated values received in response to the operations
          performed on the values for the query sent.

        Example output:
          .. code-block:: python

            {
                'sum': 18.05674,
                'max': 6.009373
             }
        """
        if not isinstance(operations, list):
            raise TypeError("Operations can be only of type list")
        if not operations:
            _LOGGER.debug("No operations found to perform")
            return None
        aggregated_values = {}
        query_values = []
        if start_time is not None and end_time is not None:
            data = self.custom_query_range(
                query=query, params=params, start_time=start_time, end_time=end_time, step=step
            )
            for result in data:
                values = result["values"]
                query_values.extend(float(val[1]) for val in values)
        else:
            data = self.custom_query(query, params)
            query_values.extend(float(result["value"][1]) for result in data)
        if not query_values:
            _LOGGER.debug("No values found for given query.")
            return None

        np_array = numpy.array(query_values)
        for operation in operations:
            if operation == "sum":
                aggregated_values["sum"] = numpy.sum(np_array)
            elif operation == "max":
                aggregated_values["max"] = numpy.max(np_array)
            elif operation == "min":
                aggregated_values["min"] = numpy.min(np_array)
            elif operation == "average":
                aggregated_values["average"] = numpy.average(np_array)
            
            elif operation.startswith("percentile"):
                percentile = float(operation.split("_")[1])
                aggregated_values[f"percentile_{percentile}"] = numpy.percentile(query_values, percentile)

            elif operation == "deviation":
                aggregated_values["deviation"] = numpy.std(np_array)
            elif operation == "variance":
                aggregated_values["variance"] = numpy.var(np_array)
            else:
                raise TypeError(f"Invalid operation: {operation}")
        return aggregated_values

    async def async_get_metric_aggregation(
        self,
        query: str,
        operations: List[str],
        start_time: datetime = None,
        end_time: datetime = None,
        step: str = "15",
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Get aggregations on metric values received from PromQL query.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query. And, a
        list of operations to perform such as- sum, max, min, deviation, etc.
        with start_time, end_time and step.

        The received query is passed to the custom_query_range method which returns
        the result of the query and the values are extracted from the result.

        :param query: (str) This is a PromQL query, a few examples can be found
          at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param operations: (list) A list of operations to perform on the values.
          Operations are specified in string type.
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
          sent along with the API request, such as "timeout"
          Available operations - sum, max, min, variance, nth percentile, deviation
          and average.

        :returns: (dict) A dict of aggregated values received in response to the operations
          performed on the values for the query sent.

        Example output:
          .. code-block:: python

            {
                'sum': 18.05674,
                'max': 6.009373
             }
        """
        if not isinstance(operations, list):
            raise TypeError("Operations can be only of type list")
        if not operations:
            _LOGGER.debug("No operations found to perform")
            return None
        aggregated_values = {}
        query_values = []
        if start_time is not None and end_time is not None:
            data = await self.async_custom_query_range(
                query=query, params=params, start_time=start_time, end_time=end_time, step=step
            )
            for result in data:
                values = result["values"]
                query_values.extend(float(val[1]) for val in values)
        else:
            data = await self.async_custom_query(query, params)
            query_values.extend(float(result["value"][1]) for result in data)
        if not query_values:
            _LOGGER.debug("No values found for given query.")
            return None

        np_array = numpy.array(query_values)
        for operation in operations:
            if operation == "sum":
                aggregated_values["sum"] = numpy.sum(np_array)
            elif operation == "max":
                aggregated_values["max"] = numpy.max(np_array)
            elif operation == "min":
                aggregated_values["min"] = numpy.min(np_array)
            elif operation == "average":
                aggregated_values["average"] = numpy.average(np_array)
            
            elif operation.startswith("percentile"):
                percentile = float(operation.split("_")[1])
                aggregated_values[f"percentile_{percentile}"] = numpy.percentile(query_values, percentile)

            elif operation == "deviation":
                aggregated_values["deviation"] = numpy.std(np_array)
            elif operation == "variance":
                aggregated_values["variance"] = numpy.var(np_array)
            else:
                raise TypeError(f"Invalid operation: {operation}")
        return aggregated_values
    
    @property
    def saved_queries(self):
        """Return saved queries."""
        return self._saved_queries
    
    def add_saved_query(self, name: str, query: str, params: Optional[Dict[str, Any]] = None):
        """Add a saved query."""
        self._saved_queries[name] = SavedQuery(
            query = query, params = params
        )
    
    def delete_saved_query(self, name: str):
        """Delete a saved query."""
        if name in self._saved_queries:
            del self._saved_queries[name]
    
    def get(
        self, 
        name: str,
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        step: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None 
    ):
        """
        Return the values for a saved query.
        """
        assert name in self._saved_queries, f"Saved query {name} not found"
        saved_query = self._saved_queries[name]
        params = params or saved_query.params
        if start_time and end_time:
            return self.custom_query_range(
                query = saved_query.query, 
                start_time = start_time, 
                end_time = end_time, 
                step = step,
                params = params
            )
        return self.custom_query(
            query = saved_query.query,
            params = params
        )

    async def async_get(
        self, 
        name: str,
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        step: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None 
    ):
        """
        Return the values for a saved query.
        """
        assert name in self._saved_queries, f"Saved query {name} not found"
        saved_query = self._saved_queries[name]
        params = params or saved_query.params
        if start_time and end_time:
            return await self.async_custom_query_range(
                query = saved_query.query, 
                start_time = start_time, 
                end_time = end_time, 
                step = step,
                params = params
            )
        
        return await self.async_custom_query(
            query = saved_query.query,
            params = params
        )