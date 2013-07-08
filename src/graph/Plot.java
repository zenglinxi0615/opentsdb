// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.graph;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.TreeMap;
import java.lang.Double;

import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;

import java.math.BigDecimal;

/**
 * Produces files to generate graphs with Gnuplot.
 * <p>
 * This class takes a bunch of {@link DataPoints} instances and generates a
 * Gnuplot script as well as the corresponding data files to feed to Gnuplot.
 */
public final class Plot {

  private static final Logger LOG = LoggerFactory.getLogger(Plot.class);

  /** Mask to use on 32-bit unsigned integers to avoid sign extension.  */
  private static final long UNSIGNED = 0x00000000FFFFFFFFL;

  /** Default (current) timezone.  */
  private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private final int start_time;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private final int end_time;

  /** All the DataPoints we want to plot. */
  private ArrayList<DataPoints> datapoints =
    new ArrayList<DataPoints>();

  /** List of global annotations */
  private List<Annotation> globals = null;

  /** Per-DataPoints Gnuplot options. */
  private ArrayList<String> options = new ArrayList<String>();

  /** Global Gnuplot parameters. */
  private Map<String, String> params;

  /** Minimum width / height allowed. */
  private static final short MIN_PIXELS = 100;

  /** Width of the graph to generate, in pixels. */
  private short width = (short) 1024;

  /** Height of the graph to generate, in pixels. */
  private short height = (short) 768;

  /**
   * Number of seconds of difference to apply in order to get local time.
   * Gnuplot always renders timestamps in UTC, so we simply apply a delta
   * to get local time.
   */
  private final short utc_offset;
 
  private Map<String, List<String>> query = null;
  
  /** max value of all the datapoints. */
  private double max = 0;

  /** min value of all the datapoints. */
  private double min = 0;

  /** current value of the datapoints. */
  private double cur = 0;

  /** average value of all the datapoints. */
  private double avg = 0;

  /**
   * Constructor.
   * @param start_time Timestamp of the start time of the graph.
   * @param end_time Timestamp of the end time of the graph.
   * @throws IllegalArgumentException if either timestamp is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   */
  public Plot(final long start_time, final long end_time) {
    this(start_time, end_time, DEFAULT_TZ);
  }

  /**
   * Constructor.
   * @param start_time Timestamp of the start time of the graph.
   * @param end_time Timestamp of the end time of the graph.
   * @param tz Timezone to use to render the timestamps.
   * If {@code null} the current timezone as of when the JVM started is used.
   * @throws IllegalArgumentException if either timestamp is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   * @since 1.1
   */
   public Plot(final long start_time, final long end_time, TimeZone tz) {
    if ((start_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid start time: " + start_time);
    } else if ((end_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid end time: " + end_time);
    } else if (start_time >= end_time) {
      throw new IllegalArgumentException("start time (" + start_time
        + ") is greater than or equal to end time: " + end_time);
    }
    this.start_time = (int) start_time;
    this.end_time = (int) end_time;
    if (tz == null) {
      tz = DEFAULT_TZ;
    }
    this.utc_offset = (short) (tz.getOffset(System.currentTimeMillis()) / 1000);
  }

  /**
   * Sets the global parameters for this plot.
   * @param params Each entry is a Gnuplot setting that will be written as-is
   * in the Gnuplot script file: {@code set KEY VALUE}.
   * When the value is {@code null} the script will instead contain
   * {@code unset KEY}.
   * <p>
   * Special parameters with a special meaning (since OpenTSDB 1.1):
   * <ul>
   * <li>{@code bgcolor}: Either {@code transparent} or an RGB color in
   * hexadecimal (with a leading 'x' as in {@code x01AB23}).</li>
   * <li>{@code fgcolor}: An RGB color in hexadecimal ({@code x42BEE7}).</li>
   * </ul>
   */
  public void setParams(final Map<String, String> params) {
    this.params = params;
  }

  /**
   * Sets the dimensions of the graph (in pixels).
   * @param width The width of the graph produced (in pixels).
   * @param height The height of the graph produced (in pixels).
   * @throws IllegalArgumentException if the width or height are negative,
   * zero or "too small" (e.g. less than 100x100 pixels).
   */
  public void setDimensions(final short width, final short height) {
    if (width < MIN_PIXELS || height < MIN_PIXELS) {
      final String what = width < MIN_PIXELS ? "width" : "height";
      throw new IllegalArgumentException(what + " smaller than " + MIN_PIXELS
                                         + " in " + width + 'x' + height);
    }
    this.width = width;
    this.height = height;
  }

  /** @param globals A list of global annotation objects, may be null */
  public void setGlobals(final List<Annotation> globals) {
    this.globals = globals;
  }

  /**
   * Adds some data points to this plot.
   * @param datapoints The data points to plot.
   * @param options The options to apply to this specific series.
   */
  public void add(final DataPoints datapoints,
                  final String options) {
    // Technically, we could check the number of data points in the
    // datapoints argument in order to do something when there are none, but
    // this is potentially expensive with a SpanGroup since it requires
    // iterating through the entire SpanGroup.  We'll check this later
    // when we're trying to use the data, in order to avoid multiple passes
    // through the entire data.
    this.datapoints.add(datapoints);
    this.options.add(options);
  }

  /**
   * Returns a view on the datapoints in this plot.
   * Do not attempt to modify the return value.
   */
  public Iterable<DataPoints> getDataPoints() {
    return datapoints;
  }

  public void setQuery(final Map<String, List<String>> query) {
    this.query = query;
  }

  /**
   * Returns a processed result when the value is larger than 1000.
   * for example: 17600 -> 17.60k.
   */
  private String convert(double num) {
    String result = "";
    if (num > 1000) {
      num = round(num / 1000, 2, BigDecimal.ROUND_DOWN);
      result = num + "k";
    } else {
      result += num;
    }
    return result;
  }

  /**
   * Returns max value of all datapoints, add by zlx.
   * Do not attempt to modify the return value.
   */
  public String getMax() {
    String max_str = convert(max); 
    return max_str;
  }

  /**
   * Returns min value of all datapoints.
   * Do not attempt to modify the return value.
   */
  public String getMin() {
    String min_str = convert(min); 
    return min_str;
  }

  /**
   * Returns current value of all datapoints.
   * Do not attempt to modify the return value.
   */
  public String getCur() {
    String cur_str = convert(cur); 
    return cur_str;
  }

  /**
   * Returns averge value of all datapoints.
   * Do not attempt to modify the return value.
   */
  public String getAvg() {
    String avg_str = convert(avg);
    return avg_str;
  }

  /**
   * Set the scale of a double value.
   * To reduce the length of title as we don't need a absolute accuracy value.
   */
  private double round(double value, int scale, int roundingMode) {
    BigDecimal bd = new BigDecimal(value);
    bd = bd.setScale(scale, roundingMode);
    double d = bd.doubleValue();
    bd = null;
    return d;
  }

  /**
   * Get the statistical information of datapoints including max, min, avg and cur value.
   * @anthor zlx 
   */
  public void setStatisticalInfo(DataPoints dp) {
    int npoints = 0;
    if (dp.size() <= 0) {
      return;
    }
    double max = -Double.MAX_VALUE;
    double min = Double.MAX_VALUE;
    double cur = 0;
    long ts_cur = 0;
    double avg = 0;
    double sum = 0;
    double value = 0;
    
    for (final DataPoint d : dp) {
      final long ts = d.timestamp();
      if (ts >= (start_time & UNSIGNED) && ts <= (end_time & UNSIGNED) ) {
        npoints++;
        if (d.isInteger()) {
          value = d.longValue();
        } else {
          value = d.doubleValue();
          if (value != value || Double.isInfinite(value)) {
            throw new IllegalStateException("NaN or Infinity found in"
                + " datapoints #" + npoints + ": " + value + " d=" + d);
          }
        }
        sum += value;
        max = max < value ? value : max;
        min = min > value ? value : min;
        cur = ts > ts_cur ? value : cur;
        ts_cur = ts > ts_cur ? ts : ts_cur;
     }
    }

    if (npoints > 0) {
       avg = sum / npoints;
    } else {
       max = 0;
       min = 0;
    }
    
    this.max = round(max, 2, BigDecimal.ROUND_DOWN);
    this.min = round(min, 2, BigDecimal.ROUND_DOWN);
    this.cur = round(cur, 2, BigDecimal.ROUND_DOWN);
    this.avg = round(avg, 2, BigDecimal.ROUND_DOWN);
  }      

  /*
   * merge the data files for transparent stack model to avoid color mixture.
   * @param basepath The base path to use.
   * @param datafiles The names of the data files that need to be plotted,
   * in the order in which they ought to be plotted.  It is assumed that
   * the ith file will correspond to the ith entry in {@code datapoints}.
   * Can be {@code null} if there's no data to plot.
   * @throws IOException if there was an error while writing the file.
   * @anthor zlx 
   */
  public void dataFilesMerge(final String basepath, final String[] datafiles) throws IOException {
    LOG.info("start merge data file... ");
    final int nseries = datapoints.size();
    Map<String, List<String>> data_map = new TreeMap<String, List<String>>(); 
    for (int i = 0; i < nseries; i++) {
      LOG.info("reading data file: " + datafiles[i]);
      FileReader reader = new FileReader(datafiles[i]);
      BufferedReader br = new BufferedReader(reader);
      try {
        String dataline = null;
        while((dataline = br.readLine()) != null) {
          String[] splits = dataline.split(" ");
          String ts = splits[0];
          String value = splits[1];
          if (data_map.containsKey(ts)) {
            List<String> dp_list = data_map.get(ts);
            dp_list.set(i, value);
          } else {
            String[] dp_array = new String[nseries];
            List<String> dp_list = Arrays.asList(dp_array);
            dp_list.set(i, value);
            data_map.put(ts, dp_list);
          }
        }
      } finally {
        br.close();
        reader.close();
      }
    }
    final String mergefile_name = basepath + "_mergefile" + ".dat";
    final FileWriter mergefile = new FileWriter(mergefile_name);
    BufferedWriter bw = new BufferedWriter(mergefile);
    Iterator it = data_map.entrySet().iterator();
    try {
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry) it.next();
        String key = (String) entry.getKey();
        List<String> value = (List<String>) entry.getValue();
        String dataline = key + " ";
        for (int j = 0; j < nseries; j++) {
          dataline += value.get(j) + " ";
        }
        bw.write(dataline);
        bw.newLine();
      }
    } finally {
      bw.flush();
      bw.close();
      mergefile.close();
    }
    LOG.info("finished merge data file, create dat file: " + mergefile_name);
  }

  /*
   * Generates the Gnuplot script and data files.
   * @param basepath The base path to use.  A number of new files will be
   * created and their names will all start with this string.
   * @return The number of data points sent to Gnuplot.  This can be less
   * than the number of data points involved in the query due to things like
   * aggregation or downsampling.
   * @throws IOException if there was an error while writing one of the files.
   */
  public int dumpToFiles(final String basepath) throws IOException {
    int npoints = 0;
    final int nseries = datapoints.size();
    //add by zlx, to support stack graphs such as memory graph in ganglia
    final String stacked = params.get("stacked");
    final String haveTotal = params.get("haveTotal");
    final String datafiles[] = nseries > 0 ? new String[nseries] : null;
    int flag = stacked == null ? 0 : 1;
    if (flag == 1) {
      LOG.info("stacked model turn on ... ");
    } else {
      LOG.info("stacked model off ... ");
    }
    Map<String, Double> last_values = new HashMap<String, Double>();
    for (int i = 0; i < nseries; i++) {
      if (i == nseries - 1 && haveTotal != null) {
        flag = 0;
      }
      datafiles[i] = basepath + "_" + i + ".dat";
      final PrintWriter datafile = new PrintWriter(datafiles[i]);
      try {
        int index = -1;
        int count = 0;
        for (final DataPoint d : datapoints.get(i)) {
          index++;
          final long ts = d.timestamp();
          if (ts >= (start_time & UNSIGNED) && ts <= (end_time & UNSIGNED)) {
            npoints++;
          }
          datafile.print(ts + utc_offset);
          datafile.print(' ');
          double value = 0;
          if (d.isInteger()) {
            //datafile.print(d.longValue());
            value = d.longValue();
          } else {
            value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity found in"
                  + " datapoints #" + i + ": " + value + " d=" + d);
            }
          }
          double tmp = 0;
          String timestamp = String.valueOf(ts);
	        if (last_values.containsKey(timestamp)) {
            tmp = last_values.get(timestamp);
	        }
          value += tmp * flag;
	        last_values.put(timestamp, new Double(value));
          datafile.print(value);
          datafile.print('\n');
        }
      } finally {
        datafile.close();
      }
    }
    if (stacked != null && nseries > 0) {
      dataFilesMerge(basepath, datafiles); 
    }

    if (npoints == 0) {
      // Gnuplot doesn't like empty graphs when xrange and yrange aren't
      // entirely defined, because it can't decide on good ranges with no
      // data.  We always set the xrange, but the yrange is supplied by the
      // user.  Let's make sure it defines a min and a max.
      params.put("yrange", "[0:10]");  // Doesn't matter what values we use.
    }
    writeGnuplotScript(basepath, datafiles);
    return npoints;
  }

  /*
   * read the color config file to set the color for metric,
   * if the color of a metric is undefined, gnuplot will set a default color.
   * @return the map which contains metric_name -> color pairs.
   * @anthor zlx 
   */
  private Map<String, String> readColorDefinedFile(String fileName) {
    Map<String, String> user_defined_color = new HashMap<String, String>();	  
    try {
      FileReader reader = new FileReader(fileName);
      BufferedReader br = new BufferedReader(reader);
      if (reader != null) {
        try {
          String line = null;
          while((line = br.readLine()) != null) {
            String[] splits = line.split(" ");
            String metric_name = splits[0];
            String color = splits[1];
            user_defined_color.put(metric_name, color);
          }
        } finally {
          br.close();
          reader.close();
        }
      }
    } catch (IOException e) {
      LOG.info(e.toString());
    } 
    return user_defined_color;
  }

  /**
   * Generates the Gnuplot script.
   * @param basepath The base path to use.
   * @param datafiles The names of the data files that need to be plotted,
   * in the order in which they ought to be plotted.  It is assumed that
   * the ith file will correspond to the ith entry in {@code datapoints}.
   * Can be {@code null} if there's no data to plot.
   */
  private void writeGnuplotScript(final String basepath,
                                  final String[] datafiles) throws IOException {
    final String script_path = basepath + ".gnuplot";
    final PrintWriter gp = new PrintWriter(script_path);
    final String font = params.remove("font");
    final int nseries = datapoints.size();
    Map<String, String> user_defined_color = null;
	  user_defined_color = readColorDefinedFile("/home/zenglinxi/user_defined_color");
    try {
      // XXX don't hardcode all those settings.  At least not like that.
      gp.append("set terminal png small size ")
        // Why the fuck didn't they also add methods for numbers?
        .append(Short.toString(width)).append(",")
	      // modified by zlx for setting font of the graphs
        .append(Short.toString((short)(height + ((nseries - 1) * 10)))).append(" font \"MSYH.TTF, " + font + "\""); 
      final String smooth = params.remove("smooth");
      final String stacked = params.remove("stacked");
      final String haveTotal = params.remove("haveTotal");
      final String fgcolor = params.remove("fgcolor");
      String bgcolor = params.remove("bgcolor");
      if (fgcolor != null && bgcolor == null) {
        // We can't specify a fgcolor without specifying a bgcolor.
        bgcolor = "xFFFFFF";  // So use a default.
      }
      if (bgcolor != null) {
        if (fgcolor != null && "transparent".equals(bgcolor)) {
          // In case we need to specify a fgcolor but we wanted a transparent
          // background, we also need to pass a bgcolor otherwise the first
          // hex color will be mistakenly taken as a bgcolor by Gnuplot.
          bgcolor = "transparent xFFFFFF";
        }
        gp.append(' ').append(bgcolor);
      }
      if (fgcolor != null) {
        gp.append(' ').append(fgcolor);
      }

      gp.append("\n"
                + "set key font \"MSYH.TTF, " + font + " \"\n"
                + "set key Left\n"
                + "set key reverse\n"
                + "set style fill transparent solid 0.5 noborder\n"
                + "set xdata time\n"
                + "set timefmt \"%s\"\n"
                + "if (GPVAL_VERSION < 4.6) set xtics rotate; else set xtics rotate right\n"
                + "set output \"").append(basepath + ".png").append("\"\n"
                + "set xrange [\"")
        .append(String.valueOf((start_time & UNSIGNED) + utc_offset))
        .append("\":\"")
        .append(String.valueOf((end_time & UNSIGNED) + utc_offset))
        .append("\"]\n");
      if (!params.containsKey("format x")) {
        gp.append("set format x \"").append(xFormat()).append("\"\n");
      }
      if (nseries > 0) {
        gp.write("set grid\n"
                 + "set style data linespoints\n");
        if (!params.containsKey("key")) {
          gp.write("set key right box\n");
        }
      } else {
        gp.write("unset key\n");
        if (params == null || !params.containsKey("label")) {
          gp.write("set label \"No data\" at graph 0.5,0.9 center\n");
        }
      }

      if (params != null) {
        for (final Map.Entry<String, String> entry : params.entrySet()) {
          final String key = entry.getKey();
          final String value = entry.getValue();
          if (value != null) {
            gp.append("set ").append(key)
              .append(' ').append(value).write('\n');
          } else {
            gp.append("unset ").append(key).write('\n');
          }
        }
      }
      for (final String opts : options) {
        if (opts.contains("x1y2")) {
          // Create a second scale for the y-axis on the right-hand side.
          gp.write("set y2tics border\n");
          break;
        }
      }

      // compile annotations to determine if we have any to graph
      final List<Annotation> notes = new ArrayList<Annotation>();
      for (int i = 0; i < nseries; i++) {
        final DataPoints dp = datapoints.get(i);
        final List<Annotation> series_notes = dp.getAnnotations();
        if (series_notes != null && !series_notes.isEmpty()) {
          notes.addAll(series_notes);
        }
      }
      if (globals != null) {
        notes.addAll(globals);
      }
      if (notes.size() > 0) {
        Collections.sort(notes);
        for(Annotation note : notes) {
          String ts = Long.toString(note.getStartTime());
          String value = new String(note.getDescription());
          gp.append("set arrow from \"").append(ts).append("\", graph 0 to \"");
          gp.append(ts).append("\", graph 1 nohead ls 3\n");
          gp.append("set object rectangle at \"").append(ts);
          gp.append("\", graph 0 size char (strlen(\"").append(value);
          gp.append("\")), char 1 front fc rgbcolor \"white\"\n");
          gp.append("set label \"").append(value).append("\" at \"");
          gp.append(ts).append("\", graph 0 front center\n");
        } 
      }

      gp.write("plot ");
      for (int i = nseries - 1; i >= 0; i--) {
        final DataPoints dp = datapoints.get(i);
        setStatisticalInfo(dp);
        final String title = dp.metricName() + "{Cur: " + getCur() + "  Min: " + getMin() + "  Max: " + getMax() + "  Avg: " + getAvg() + "}";
        final String mergefile_name = basepath + "_mergefile" + ".dat";
        if (stacked == null || i == 0) {
          gp.append(" \"").append(datafiles[i]).append("\" using 1:2");
        } else if (i == nseries - 1 && haveTotal != null) {
            gp.append(" \"").append(mergefile_name).append("\" using 1:" + (i + 2));
        } else {
            gp.append(" \"").append(mergefile_name).append("\" using 1:" + (i + 1) + ":" + (i + 2));
        }
        if (smooth != null) {
          gp.append(" smooth ").append(smooth);
        }
        //add by zlx, just for one especial case
        //some metrics only have two values 0 or 1
        if (dp.metricName().equals("ping")) {
          gp.append(" with steps ");
        }
        // if we need stacked graph, we should decide which area will be filled.
        if (stacked != null  && (i != nseries -1 || haveTotal == null)) {
          if (i == 0) {
            gp.append(" w filledcurves x1");
          } else {
            //this will fill the area between two curves
            gp.append(" w filledcurves ");
          }
     	  // if there are only one curves, we prefer to fill it with green
          if (nseries == 1) {
            gp.append(" lc 2 ");
          } else if (user_defined_color != null && user_defined_color.containsKey(dp.metricName())) {
              gp.append(" lc " + user_defined_color.get(dp.metricName()) + " ");
          }
          gp.append(" lw 2 title \"").append(title).write('"');
        } else {
          gp.append(" pt 0 lw 2 title \"").append(title).write('"');
        }
        final String opts = options.get(i);
        if (!opts.isEmpty()) {
          gp.append(' ').write(opts);
        }
        //if (i != nseries - 1) {
        if (i != 0) {
          gp.print(", \\");
        }
        gp.write('\n');
      }
      if (nseries == 0) {
        //gp.write('0');
        //add by zlx, if the graph has no data, we also want to show the titles
        List<String> metrics = query.get("m");
        int size = metrics.size();
        for(int i = 0; i < size; i++) {
          String metric_name = metrics.get(i).split(":")[2];
          gp.append(" 0 pt 0 lw 2 title \"").append(metric_name).write('"');
          if (i != size - 1) {
            gp.print(", \\");
          }
          gp.write('\n');
        }
      }
    } finally {
      gp.close();
      LOG.info("Wrote Gnuplot script to " + script_path);
    }
  }

  /**
   * Finds some sensible default formatting for the X axis (time).
   * @return The Gnuplot time format string to use.
   */
  private String xFormat() {
    long timespan = (end_time & UNSIGNED) - (start_time & UNSIGNED);
    if (timespan < 2100) {  // 35m
      return "%H:%M:%S";
    } else if (timespan < 86400) {  // 1d
      return "%H:%M";
    } else if (timespan < 604800) {  // 1w
      return "%a %H:%M";
    } else if (timespan < 1209600) {  // 2w
      return "%a %d %H:%M";
    } else if (timespan < 7776000) {  // 90d
      return "%b %d";
    } else {
      return "%Y/%m/%d";
    }
  }

}
