"""
A Class for metric object
"""
from copy import deepcopy
import dateparser
import pandas

try:
    import matplotlib.pyplot as plt
    from pandas.plotting import register_matplotlib_converters

    register_matplotlib_converters()
    _MPL_FOUND = True
except ImportError as exce:
    _MPL_FOUND = False
    print("WARNING: Plotting will not work as matplotlib was not found")


class Metric:
    """
    A Class for `Metric` object

    :param metric: (dict) A metric item from the list of metrics received from prometheus
    :param oldest_data_datetime: (str) So any metric values in the dataframe that are older than
                    this value will be deleted when new data is added to the dataframe using
                    the __add__("+") operator.
                    Example: oldest_data_datetime="10d", will delete the metric data that is older
                            than 10 days. The dataframe is pruned only when new data is added to it.

                             oldest_data_datetime="23 May 2019 12:00:00"

                             oldest_data_datetime="1561475156" can be set using the unix timestamp

    Example Usage:
        ``prom = PrometheusConnect()``

        ``my_label_config = {'cluster': 'my_cluster_id', 'label_2': 'label_2_value'}``

        ``metric_data = prom.get_metric_range_data(metric_name='up', label_config=my_label_config)``
        ``Here metric_data is a list of metrics received from prometheus``

        ``my_metric_object = Metric(metric_data[0], "10d") # only for the first item in the list``

    """

    def __init__(self, metric, oldest_data_datetime=None):
        """
        Constructor for the Metric object

        """
        self.metric_name = metric["metric"]["__name__"]
        self.label_config = deepcopy(metric["metric"])
        self.oldest_data_datetime = oldest_data_datetime
        del self.label_config["__name__"]

        # if it is a single value metric change key name
        if "value" in metric:
            metric["values"] = [metric["value"]]

        self.metric_values = pandas.DataFrame(metric["values"], columns=["ds", "y"]).apply(
            pandas.to_numeric, args=({"errors": "coerce"})
        )
        self.metric_values["ds"] = pandas.to_datetime(self.metric_values["ds"], unit="s")

    def __eq__(self, other):
        """
        overloading operator `=`

        Check whether two metrics are the same (are the same time-series regardless of their data)

        :return: (bool) If two Metric objects belong to the same time-series,
                 i.e. same name and label config, it will return True, else False

        Example Usage:
            ``metric_1 = Metric(metric_data_1)``

            ``metric_2 = Metric(metric_data_2)``

            ``print(metric_1 == metric_2) # will print True if they belong to the same time-series``

        """
        return bool(
            (self.metric_name == other.metric_name) and (self.label_config == other.label_config)
        )

    def __str__(self):
        """
        This will make it print in a cleaner way when print function is used on a Metric object

        Example Usage:
            ``metric_1 = Metric(metric_data_1)``

            ``print(metric_1) # will print the name, labels and the head of the dataframe``

        """
        name = "metric_name: " + repr(self.metric_name) + "\n"
        labels = "label_config: " + repr(self.label_config) + "\n"
        values = "metric_values: " + repr(self.metric_values)

        return "{" + "\n" + name + labels + values + "\n" + "}"

    def __add__(self, other):
        """
        overloading operator `+`
        Add two metric objects for the same time-series

        Example Usage:
            ``metric_1 = Metric(metric_data_1)``

            ``metric_2 = Metric(metric_data_2)``

            ``metric_12 = metric_1 + metric_2`` # will add the data in metric_2 to metric_1
                                                # so if any other parameters are set in metric_1
                                                # will also be set in metric_12
                                                # (like `oldest_data_datetime`)
        """
        if self == other:
            new_metric = deepcopy(self)
            new_metric.metric_values = new_metric.metric_values.append(
                other.metric_values, ignore_index=True
            )
            new_metric.metric_values = new_metric.metric_values.dropna()
            new_metric.metric_values = (
                new_metric.metric_values.drop_duplicates("ds")
                .sort_values(by=["ds"])
                .reset_index(drop=True)
            )
            # if oldest_data_datetime is set, trim the dataframe and only keep the newer data
            if new_metric.oldest_data_datetime:
                # create a time range mask
                mask = new_metric.metric_values["ds"] >= dateparser.parse(
                    str(new_metric.oldest_data_datetime)
                )
                # truncate the df within the mask
                new_metric.metric_values = new_metric.metric_values.loc[mask]

            return new_metric

        if self.metric_name != other.metric_name:
            error_string = "Different metric names"
        else:
            error_string = "Different metric labels"
        raise TypeError("Cannot Add different metric types. " + error_string)

    def plot(self):

        if _MPL_FOUND:
            fig, axis = plt.subplots()
            axis.plot_date(self.metric_values.ds, self.metric_values.y, linestyle=":")
            fig.autofmt_xdate()
        # if matplotlib was not imported
        else:
            raise ImportError("matplotlib was not found")
