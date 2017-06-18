/*
 * Copyright 2014 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.iu.wordcount;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.iu.harp.keyval.IntCountVal;
import edu.iu.harp.keyval.IntCountValPlus;
import edu.iu.harp.keyval.Key;
import edu.iu.harp.keyval.KeyValPartition;
import edu.iu.harp.keyval.KeyValTable;
import edu.iu.harp.keyval.StringKey;
import edu.iu.harp.keyval.Value;

public class CollectiveWordCount {

  public static class WordCountMapper
    extends
    CollectiveMapper<Object, Text, Text, IntWritable> {

    private static final Log LOG = LogFactory
      .getLog(WordCountMapper.class);

    public void mapCollective(
      KeyValReader reader, Context context)
      throws IOException, InterruptedException {
      // Read keyvals and combine
      KeyValTable<StringKey, IntCountVal> table =
        new KeyValTable<>(StringKey.class,
          IntCountVal.class,
          new IntCountValPlus(),
          this.getNumWorkers(),
          this.getResourcePool());
      while (reader.nextKeyValue()) {
        Text value = reader.getCurrentValue();
        StringTokenizer itr =
          new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          String strKey = itr.nextToken();
          table.addKeyVal(new StringKey(strKey),
            new IntCountVal(1, 1));
        }
      }
      // Regroup and combine
      boolean isSuccess =
        this.regroup("main", "regroup-combine",
          table);
      LOG
        .info("Is regroup success: " + isSuccess);
      // Write out
      Text word = new Text();
      IntWritable count = new IntWritable(0);
      for (KeyValPartition partition : table
        .getPartitions()) {
        for (Entry<Key, Value> entry : partition
          .getKeyVals()) {
          LOG.info(((StringKey) entry.getKey())
            .getStringKey()
            + " "
            + ((IntCountVal) entry.getValue())
              .getIntValue());
          word.set(((StringKey) entry.getKey())
            .getStringKey());
          count.set(((IntCountVal) entry
            .getValue()).getIntValue());
          context.write(word, count);
        }
      }
    }
  }

  public static void main(String[] args)
    throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =
      new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err
        .println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job =
      new Job(conf, "collective word count");
    JobConf jobConf =
      (JobConf) job.getConfiguration();
    jobConf.set("mapreduce.framework.name",
      "map-collective");
    jobConf.setNumReduceTasks(0);
    job.setJarByClass(CollectiveWordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(
      otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(
      otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0
      : 1);
  }
}
