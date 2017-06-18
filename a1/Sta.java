import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sta {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    //private final static IntWritable one = new IntWritable(1); // type of output value
    //private Text std = new Text("std");
	//private Text min = new Text("min");
	//private Text max = new Text("max");
	//private Text avg = new Text("avg");
    private Text sum = new Text("sum");  
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
          
      while (itr.hasMoreTokens()) {
	    double v = Double.parseDouble(itr.nextToken());
        //word.set(itr.nextToken());    // set word as each input keyword
        context.write(sum, new DoubleWritable(v));     // create a pair <keyword, 1>

      }
    }
  }
  
  public static class Reduce
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0; // initialize the sum for each keyword
	  double avg = 0, min = 2, max = 0, std = 0, variance = 0;
	  double count  = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
		if ( val.get() > max) { max = val.get();}
		if ( val.get() < min) { min = val.get();}
		count += 1;
      }
	  avg = sum / count;
	  for (DoubleWritable val : values) {
	    double base = val.get();
	    variance  +=  (base - avg)*(base - avg) / count;
	  }
      std = Math.sqrt(variance); 
      context.write(new Text("avg"), new DoubleWritable(avg)); // create a pair <keyword, number of occurences>
	  context.write(new Text("min"), new DoubleWritable(min));
	  context.write(new Text("max"), new DoubleWritable(max));
	  context.write(new Text("std"), new DoubleWritable(std));
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	//conf.set("textinputformat.record.delimiter","------------");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "sta");
	//job.setInputFormat(TextInputFormat.class);

    job.setJarByClass(Sta.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // Add a combiner here, not required to successfully run the wordcount program  
    //job.setCombinerClass(Reduce.class);
    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(DoubleWritable.class);
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
