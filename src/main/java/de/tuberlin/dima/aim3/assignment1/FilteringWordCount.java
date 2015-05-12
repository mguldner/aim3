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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class FilteringWordCount extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
        Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(line.toString());
      while(st.hasMoreTokens()) {
        String word = st.nextToken().toLowerCase();
        String[] stopWords = {"to", "and", "in", "the"};
        boolean isStopWord = false;
        for (int i = 0; i < stopWords.length; i++) {
          if(word.equals(stopWords[i]))
            isStopWord=true;
        }
        if (!isStopWord) {
          if (word.charAt(word.length() - 1) < 97 || word.charAt(word.length() - 1) > 122)
            word = word.substring(0, word.length() - 1);
          ctx.write(new Text(word), new IntWritable(1));
        }
      }
    }
  }

  static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable i : values) {
        sum += i.get();
      }
      ctx.write(key, new IntWritable(sum));
    }
  }

}