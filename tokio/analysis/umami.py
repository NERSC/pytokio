#!/usr/bin/env python
"""
Class and tools to generate TOKIO UMAMI plots
"""

import json
import numpy
import pandas
import textwrap
import datetime
import collections
import matplotlib
import matplotlib.pyplot

DEFAULT_LINEWIDTH  = 1
DEFAULT_LINECOLOR  = "#853692"
DEFAULT_FONTSIZE   = 12
DEFAULT_COLORSCALE = [ '#DA0017', '#FD6A07', '#40A43A', '#2C69A9' ]
DEFAULT_FIGSIZE    = (6.0, 12.0 / 9.0)

class Umami(collections.OrderedDict):
    """
    Subclass of dictionary that stores all of the data needed to generate an
    UMAMI diagram.  It is keyed by a metric name, and values are UmamiMetric
    objects which contain timestamps (x values) and measurements (y values)
    """
    def __init__(self, *args, **kwargs):
        super(Umami, self).__init__(*args, **kwargs)

    def to_dict(self):
        """
        Convert this object _and all of its constituent UmamiMetric objects_
        into a dictionary
        """
        return { k: v.__dict__ for k, v in self.iteritems() }

    def _to_dict_for_pandas(self, stringify_key=False):
        """
        Convert this object into a DataFrame, indexed by timestamp, with each
        column as a metric.  The Umami attributes (labels, etc) are not
        expressed.
        """
        to_df = {}
        for metric, measurement in self.iteritems():
            for index, timestamp in enumerate(measurement.timestamps):
                if stringify_key:
                    key = str(timestamp)
                else:
                    key = timestamp

                if key not in to_df:
                    to_df[key] = {}
                to_df[key].update({metric: measurement.values[index]})
        return to_df

    def to_json(self):
        return json.dumps(self._to_dict_for_pandas(stringify_key=True), indent=4, sort_keys=True)

    def to_dataframe(self):
        return pandas.DataFrame.from_dict(self._to_dict_for_pandas(), orient='index')

    def plot(self, output_file=None,
             highlight_index=-1,
             linewidth=DEFAULT_LINEWIDTH,
             linecolor=DEFAULT_LINECOLOR,
             colorscale=DEFAULT_COLORSCALE,
             fontsize=DEFAULT_FONTSIZE,
             figsize=DEFAULT_FIGSIZE):
        """Create a graphical representation of the UMAMI object

        Args:
            output_file (str or None): save umami diagram to file of given name
            highlight_index (int): index of measurement to highlight
            linewidth (int): linewidth for both timeseries and boxplot lines
            linecolor (str): color of line in timeseries panels
            colorscale (list of str): colors to use for data below the 25th,
                50th, 75th, and 100th percentiles
            fontsize (int): font size for UMAMI labels
            figsize (tuple of float): x, y dimensions of a single UMAMI row;
                multiplied by len(self.keys()) to determine full diagram height

        Returns:
            List of axes corresponding to each panel in the UMAMI diagram
        """
        rows_to_plot = self.keys()
        fig = matplotlib.pyplot.figure()
        fig.set_size_inches(figsize[0], len(rows_to_plot) * figsize[1])

        # Required to adjust the column widths of our figure (width_ratios)
        gridspec = matplotlib.gridspec.GridSpec(
            len(rows_to_plot),  # how many rows to draw
            2,                  # how many columns to draw
            width_ratios=[4,1]) # ratio of column widths

        # Get the full range of x so we can force all rows to share the same
        # x range in the presence of trailing/leading NaNs
        x_min = None
        x_max = None
        for measurement in self.itervalues():
            this_min = min(measurement.timestamps)
            this_max = max(measurement.timestamps)
            if x_min is None or this_min < x_min:
                x_min = this_min
            if x_max is None or this_max > x_max:
                x_max = this_max

        # Draw UMAMI rows
        last_ax_ts = None
        row_num = None
        for metric, measurement in self.iteritems():
            if row_num is None:
                row_num = 0
            else:
                row_num += 1

            x = measurement.timestamps
            y = measurement.values
    
            ### first plot the timeseries of the given variable
            ax_ts = fig.add_subplot(gridspec[2*row_num])
            ax_ts.plot(x, y,
                       linestyle='-',
                       marker='x',
                       linewidth=linewidth,
                       color=linecolor)
    
            # textwrap.wrap inserts line breaks into each label
            ax_ts.set_ylabel('\n'.join(textwrap.wrap(
                                text=measurement.label,
                                    width=15,
                                    break_on_hyphens=True)),
                                fontsize=fontsize,
                                rotation=0,
                                horizontalalignment='right',
                                verticalalignment='center')
            ax_ts.grid()
            ax_ts.set_xlim(x_min, x_max)
    
            # blank out the labels for all subplots except the bottom-most one
            if row_num != len(rows_to_plot) - 1:
                ax_ts.set_xticklabels([])
            else:
                last_ax_ts = ax_ts
                # resize and rotate the labels for the timeseries plot
                for tick in ax_ts.xaxis.get_major_ticks():
                    tick.label.set_fontsize(fontsize) 
                    tick.label.set_rotation(45)
    
            # also adjust the font size for the y labels
            for tick in ax_ts.yaxis.get_major_ticks():
                tick.label.set_fontsize(fontsize)
    
            # then plot the boxplot summary of the given variable
            ax_box = fig.add_subplot(gridspec[2*row_num + 1])
            y_box_data = numpy.array(y)
            y_box_mask = [True] * len(y_box_data)
            y_box_mask[highlight_index] = False
            y_box_data = y_box_data[y_box_mask]
            y_box_data = y_box_data[~numpy.isnan(y_box_data)]
            boxp = ax_box.boxplot(y_box_data, # note: do not include last measurement in boxplot
                           widths=0.70,
                           boxprops={'linewidth':linewidth},
                           medianprops={'linewidth':linewidth},
                           whiskerprops={'linewidth':linewidth},
                           capprops={'linewidth':linewidth},
                           flierprops={'linewidth':linewidth},
                           whis=[5,95])
    
            # scale the extents of the y ranges a little for clarity
            orig_ylim = ax_ts.get_ylim()
            new_ylim = map(lambda a, b: a*(1 + b), orig_ylim, (-0.1, 0.1))
            ax_ts.set_ylim(new_ylim)
            
            yticks = ax_ts.get_yticks().tolist()
            
            # the following is a heuristic to determine how close the topmost
            # tick label is to the edge of the plot.  if it's too close, blank
            # it out so it doesn't overlap with the bottom-most tick label
            # of the row above it
            critical_fraction = abs(1.0 - (yticks[-1] - new_ylim[0]) / (new_ylim[-1] - new_ylim[0]))
            if row_num > 0 and critical_fraction < 0.01:
                # note that setting one of the yticks to a string resets the
                # formatting so that the tick labels appear as floats.  since
                # we (hopefully) would get integral ticks otherwise, force
                # them to ints.  This will mess things up if the yrange is
                # very narrow and must be expressed as floats.
                yticks = map(int, yticks)
                yticks[-1] = " "
                ax_ts.set_yticklabels(yticks)
                            
            # lock in the y range to match the timeseries plot, just in case
            ax_box.set_ylim(ax_ts.get_ylim())
    
            # determine the color of our highlights based on quartile
            percentiles = [ numpy.nanpercentile(y[0:-1], percentile) for percentile in 25, 50, 75, 100 ]
            for color_index, percentile in enumerate(percentiles):
                if y[highlight_index] <= percentile:
                    break
            if measurement.big_is_good:
                highlight_color = colorscale[color_index]
            else:
                highlight_color = colorscale[(1+color_index)*-1]
    
            # highlight the latest measurement on the timeseries plot
            x_last = matplotlib.dates.date2num(x[highlight_index])
            x_2nd_last = matplotlib.dates.date2num(x[highlight_index-1])
            ax_ts.plot([x_2nd_last, x_last],
                       [y[highlight_index-1], y[highlight_index]],
                       linestyle='-',
                       color=highlight_color,
                       linewidth=linewidth*2.0)
            ax_ts.plot([x_last], [y[highlight_index]],
                       marker='*',
                       color=highlight_color,
                       markersize=15)
    
            # where does this last data point lie on the distribution?
            ax_box.plot([0,2],
                        [y[highlight_index],y[highlight_index]],
                        linestyle='--',
                        color=highlight_color,
                        linewidth=2.0,
                        zorder=10)
    
            # blank out all labels
            ax_box.set_yticklabels([""])
            ax_box.set_xticklabels([""])
            ax_box.yaxis.grid()
    
        fig.subplots_adjust(hspace=0.0, wspace=0.0)
        fig.autofmt_xdate()
        last_ax_ts.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%b %d'))
    
        if output_file is not None:
            fig.savefig(output_file, bbox_inches="tight")

        return fig.axes

class UmamiMetric(object):
    """
    A single row of an UMAMI diagram.  Logically contains timeseries data from
    a single connector, where the 'timestamps' attribute is a list of timestamps
    (seconds since epoch), and the 'values' attribute is a list of values
    corresponding to each timestamp.  The number of timestamps and attributes
    must always be the same.
    """
    def __init__(self, timestamps, values, label, big_is_good=True):
        # If we are given pandas.Series, convert them to lists, then copy.
        # Otherwise, just copy the list-like inputs.
        if type(timestamps) == pandas.Series:
            self.timestamps = timestamps.tolist()[:]
        else:
            self.timestamps = timestamps[:]

        if type(values) == pandas.Series:
            self.values = values.tolist()[:]
        else:
            self.values = values[:]

        self.label = label
        self.big_is_good = big_is_good
        if len(self.timestamps) != len(self.values):
            raise Exception('timestamps and values must be of equal length')

    def to_json(self):
        return json.dumps(self.__dict__, default=_serialize_datetime)
 
    def append(self, timestamp, value):
        """
        Can only add values along with a timestamp.
        """
        self.timestamps.append(timestamp)
        self.values.append(value)

    def pop(self):
        """
        Analogous to the list .pop() method.
        """
        t = self.timestamps.pop()
        v = self.values.pop()
        return t, v

def _serialize_datetime(obj):
    """
    Special serializer function that converts datetime into something that can
    be encoded in json
    
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        serial = obj.isoformat()
        return (obj - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    raise TypeError ("Type %s not serializable" % type(obj))
