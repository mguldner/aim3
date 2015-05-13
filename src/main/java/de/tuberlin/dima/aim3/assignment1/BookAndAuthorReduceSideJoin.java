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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String, String> parsedArgs = parseArgs(args);
    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job reduceSideJoin = new Job(new Configuration(getConf()));
    Configuration reduceSideJoinConf = reduceSideJoin.getConfiguration();
    reduceSideJoin.setInputFormatClass(TextInputFormat.class);

    MultipleInputs.addInputPath(reduceSideJoin, books, TextInputFormat.class, ReduceSideJoinBookMapper.class);
    MultipleInputs.addInputPath(reduceSideJoin, authors, TextInputFormat.class, ReduceSideJoinAuthorMapper.class);
    reduceSideJoin.setMapOutputKeyClass(IndexAndGroup.class);
    reduceSideJoin.setMapOutputValueClass(Text.class);

    reduceSideJoinConf.setBoolean("mapred.compress.map.output", true);

    reduceSideJoin.setReducerClass(ReduceSideJoinReducer.class);
    reduceSideJoin.setOutputKeyClass(Text.class);
    reduceSideJoin.setOutputValueClass(Text.class);

    reduceSideJoin.setPartitionerClass(ReduceSideJoinPartitioner.class);
    reduceSideJoin.setGroupingComparatorClass(ReduceSideJoinComparator.class);

    reduceSideJoin.setOutputFormatClass(TextOutputFormat.class);
    reduceSideJoinConf.set("mapred.output.dir", outputPath.toString());
    reduceSideJoin.waitForCompletion(true);

    return 0;
  }

  static class ReduceSideJoinBookMapper extends Mapper<Object, Text, IndexAndGroup, Text> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer stBook = new StringTokenizer(line.toString());
      Integer indexBook = Integer.parseInt(stBook.nextToken());
      String lineString = line.toString().substring(1);
      while(lineString.charAt(0) == ' '){
        lineString = lineString.substring(1);
      }
      ctx.write(new IndexAndGroup(new Text(indexBook.toString()), new Text("book")), new Text(lineString));
    }
  }

  static class ReduceSideJoinAuthorMapper extends Mapper<Object, Text, IndexAndGroup, Text> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer stAuthor = new StringTokenizer(line.toString());
      Integer indexAuthor = Integer.parseInt(stAuthor.nextToken());
      String a = "";
      while (stAuthor.hasMoreTokens())
        a += " " + stAuthor.nextToken();
      a = a.substring(1);
      Text author = new Text(a);
      ctx.write(new IndexAndGroup(new Text(indexAuthor.toString()), new Text("author")), author);
    }
  }

  static class ReduceSideJoinReducer extends Reducer<IndexAndGroup, Text, Text, Book> {
    @Override
    protected void reduce(IndexAndGroup key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {
      ArrayList<Book> books = new ArrayList<Book>();
      Text author = null;
      for(Text t : values){
        if(key.getGroup().equals(new Text("author"))){
          author=new Text(t.toString());
        }
        else{
          StringTokenizer st = new StringTokenizer(t.toString());
          int year = Integer.parseInt(st.nextToken());
          String title="";
          //Get the name of the book, the first caracter is a blank space
          while (st.hasMoreTokens())
            title+= " "+st.nextToken();
          //Remove the blank space
          title=title.substring(1);
          Book b = new Book(title, year);
          books.add(b);
        }
      }
      for(Book b : books) {
        ctx.write(author, b);
      }
    }
  }

  static class ReduceSideJoinPartitioner extends Partitioner<IndexAndGroup, Text>{

    @Override
    public int getPartition(IndexAndGroup indexAndGroup, Text text, int numPartitions) {
      return indexAndGroup.hashCode() % numPartitions;
    }
  }

  static class ReduceSideJoinComparator extends WritableComparator{

    protected ReduceSideJoinComparator() {
      super(IndexAndGroup.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      IndexAndGroup left = (IndexAndGroup)a;
      IndexAndGroup right = (IndexAndGroup)b;
      return left.getIndex().toString().compareTo(right.getIndex().toString());
    }
  }
}

class IndexAndGroup implements WritableComparable<IndexAndGroup> {
  private Text index;
  private Text group;
  public IndexAndGroup(){
    this.index=new Text();
    this.group=new Text();
  }
  public IndexAndGroup(Text index, Text group){
    this.index = index;
    this.group = group;
  }
  public Text getGroup() {
    return group;
  }
  public Text getIndex() {
    return index;
  }
  @Override
  public int compareTo(IndexAndGroup o) {
    int comparaison = this.getIndex().compareTo(o.getIndex());
    if(comparaison==0){
      comparaison=this.getGroup().compareTo(o.getGroup());
    }
    return comparaison;
  }
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    this.index.write(dataOutput);
    this.group.write(dataOutput);
  }
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.index.readFields(dataInput);
    this.group.readFields(dataInput);
  }
  @Override
  public String toString() {
    return this.index.toString() + "\t" + this.group.toString();
  }
  public int hashCode() {
    String code = this.index.toString()+this.group.toString();
    return code.hashCode();
  }
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof  IndexAndGroup){
      IndexAndGroup o = (IndexAndGroup)obj;
      if(o.getGroup()==this.getGroup() && o.getIndex() == this.getIndex())
        return true;
    }
    return false;
  }
}