/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class AverageTemperaturePerMonth extends HadoopJob {

  private static double minimumQual;

  @Override
  public int run(String[] args) throws Exception {
    Map<String, String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));
    minimumQual = minimumQuality;

    Job averageTemp = prepareJob(inputPath, outputPath, TextInputFormat.class, AverageTempMapper.class,
            YearAndMonth.class, TempAndQuality.class, AverageTempReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);
    averageTemp.waitForCompletion(true);

    return 0;
  }

  static class AverageTempMapper extends Mapper<Object, Text, YearAndMonth, TempAndQuality> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(line.toString());
      ctx.write(new YearAndMonth(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken())),
              new TempAndQuality(Integer.parseInt(st.nextToken()), Double.parseDouble(st.nextToken())));
    }
  }

  static class AverageTempReducer extends Reducer<YearAndMonth, TempAndQuality, Text, DoubleWritable> {
    @Override
    protected void reduce(YearAndMonth key, Iterable<TempAndQuality> values, Context ctx)
            throws IOException, InterruptedException {
      double tempAv = 0;
      double number = 0;
      for (TempAndQuality i : values) {
        if (i.getQuality() > minimumQual) {
          tempAv += i.getTemperature();
          number++;
        }
      }
      tempAv /= number;
      ctx.write(new Text(key.toString()), new DoubleWritable(tempAv));
    }
  }
}

class TempAndQuality implements Writable {
  private int temperature;
  private double quality;

  public TempAndQuality(){
    this.temperature=0;
    this.quality=0.0;
  }

  public TempAndQuality(int temperature, double quality) {
    this.temperature = temperature;
    this.quality = quality;
  }

  public double getQuality() {
    return quality;
  }

  public int getTemperature() {
    return temperature;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.temperature);
    out.writeDouble(this.quality);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.temperature = in.readInt();
    this.quality = in.readDouble();
  }
}

class YearAndMonth implements WritableComparable {

  private int year;
  private int month;

  public YearAndMonth(){
    this.year=0;
    this.month=0;
  }

  public YearAndMonth(int year, int month) {
    this.year = year;
    this.month = month;
  }

  public int getYear() {
    return this.year;
  }

  public int getMonth() {
    return month;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof YearAndMonth) {
      YearAndMonth other = (YearAndMonth) o;
      return year == other.year && month == other.month;
    }
    return false;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(this.year);
    out.writeInt(this.month);
  }

  public void readFields(DataInput in) throws IOException {
    this.year = in.readInt();
    this.month = in.readInt();
  }

  public int compareTo(Object o) {
    if (o instanceof YearAndMonth) {
      YearAndMonth other = (YearAndMonth) o;
      if (this.year == other.year)
        return Integer.compare(this.month, other.month);
      else
        return Integer.compare(this.year, other.year);
    } else return 1;
  }

  @Override
  public int hashCode() {
    return 31 * this.month + this.year;
  }

  @Override
  public String toString() {
    return this.year+"\t"+this.month;
  }
}

/*class Month implements WritableComparable{
  public int month;
  public int year;

  public Month(int year, int month){
    this.month=month;
    this.year=year;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(this.year);
    out.writeInt(this.month);
  }

  public void readFields(DataInput in) throws IOException {
    this.year = in.readInt();
    this.month = in.readInt();
  }

  public int compareTo(Object o){
    if(o instanceof Month) {
      Month other = (Month)o;
      if (this.year == other.year)
        return Integer.compare(this.month, other.month);
      else
        return Integer.compare(this.year, other.year);
    }
    else return 0;
  }

  public boolean equals(Object o){
    if(!(o instanceof  Month))
      return false;

    Month other = (Month)(o);
    return (this.year==other.year && this.month==other.month);
  }
}*/