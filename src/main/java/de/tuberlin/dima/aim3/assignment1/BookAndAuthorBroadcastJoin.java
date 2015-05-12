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

import com.google.common.base.Preconditions;
import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

public class BookAndAuthorBroadcastJoin extends HadoopJob {
  public static HashMap<Integer, ArrayList<Book>> ht;

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    //Get the paths to the files
    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    //Store the books in a HashMap
    ht = new HashMap<Integer, ArrayList<Book>>();
    BufferedReader br = null;
    String line = null;
    try {
      //Read the file "books"
      br = new BufferedReader(new FileReader(new File(books.toString())));
      while ((line = br.readLine()) != null) {
        //Parse the file and store the elements
        StringTokenizer st = new StringTokenizer(line);
        Integer index = Integer.parseInt(st.nextToken());
        int bookYear = Integer.parseInt(st.nextToken());
        String bookTitle="";

        //Get the name of the book, the first caracter is a blank space
        while (st.hasMoreTokens())
          bookTitle+= " "+st.nextToken();
        //Remove the blank space
        bookTitle=bookTitle.substring(1);
        Book actualBook = new Book(bookTitle, bookYear);

        //If the key of the author is already stored, just add the book to the ArrayList
        if(ht.containsKey(index)){
          ht.get(index).add(actualBook);
        }
        //Else create the ArrayList for the author and put the pair
        else {
          ArrayList<Book> actualBooks = new ArrayList<Book>();
          actualBooks.add(actualBook);
          ht.put(index, actualBooks);
        }
      }
    }catch (Exception e){
      e.printStackTrace();
    }

    //Create the job
    Job broadcastJoin = prepareJob(authors, outputPath, TextInputFormat.class, BroadcastJoinMapper.class,
            Text.class, Book.class, BroadcastJoinReducer.class, Text.class, Book.class, TextOutputFormat.class);
    /* Remove the reducer part */
    broadcastJoin.setNumReduceTasks(0);
    broadcastJoin.waitForCompletion(true);

    return 0;
  }

  static class BroadcastJoinMapper extends Mapper<Object, Text, Text, Book> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(line.toString());
      //Store the index and the corresponding author
      Integer index = Integer.parseInt(st.nextToken());
      String a="";
      while(st.hasMoreTokens())
        a+=" "+st.nextToken();
      a=a.substring(1);
      Text author = new Text(a);

      //For every book of the author [index], add the pair (author, book)
      for(Book b : ht.get(index))
        ctx.write(author, b);
    }
  }

  static class BroadcastJoinReducer extends Reducer<Text, Book, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Book> values, Context ctx)
            throws IOException, InterruptedException {
      //No need of reduce function because all is done in the map function
    }
  }
}

// Object Book which implements the interface Writable
class Book implements Writable{

  private String title;
  private int year;

  public Book(){
    this.title="";
    this.year=0;
  }
  public Book(String title, int year) {
    this.title = Preconditions.checkNotNull(title);
    this.year = year;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Book) {
      Book other = (Book) o;
      return title.equals(other.title) && year == other.year;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * title.hashCode() + year;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.year);
    out.writeChars(this.title);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public String toString() {
    return this.title + "\t" + this.year ;
  }
}
