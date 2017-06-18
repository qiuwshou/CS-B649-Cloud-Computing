package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

// each map task handles one line within an adjacency matrix file
// key: file offset
// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		
		    int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			StringBuffer sb = new StringBuffer();
			// instance an object that records the information for one webpage
			RankRecord rrd = new RankRecord(line);
			int sourceUrl, targetUrl;
			sourceUrl = rrd.sourceUrl;
			// double rankValueOfSrcUrl;
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this webpage; 
				// scatter its rank value to all other urls
				sb.append("#");
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl))); 
				}
			} else {
			    int out_degree = rrd.targetUrlsList.size();
				//calculate rankValuePerTargetUrl
				double update_pr = rrd.rankValue / out_degree;
				for(int i=0; i< out_degree; i++){
				  targetUrl = rrd.targetUrlsList.get(i);
				  //System.out.println("target" + targetUrl);
				  String out_pr = Double.toString(update_pr);
				  context.write(new LongWritable(targetUrl), new Text(out_pr));
				  sb.append("#"+targetUrl );
				}
			}
			context.write(new LongWritable(sourceUrl), new Text(sb.toString()));
	} // end map

}
