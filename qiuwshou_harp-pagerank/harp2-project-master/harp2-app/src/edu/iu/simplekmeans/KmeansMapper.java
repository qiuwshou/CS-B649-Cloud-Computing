package edu.iu.simplekmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.lang.Math;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.array.DoubleArrPlus;
import edu.iu.harp.trans.DoubleArray;

public class KmeansMapper  extends CollectiveMapper<String, String, Object, Object> {
	private int vectorSize;
	private int iteration;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("start setup" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		vectorSize =configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
    	iteration = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);
    	long endTime = System.currentTimeMillis();
    	LOG.info("config (ms) :" + (endTime - startTime));
	}

	protected void mapCollective( KeyValReader reader, Context context) throws IOException, InterruptedException {
		LOG.info("Start collective mapper.");
	    long startTime = System.currentTimeMillis();
	    List<String> pointFiles = new ArrayList<String>();
	    while (reader.nextKeyValue()) {
	    	String key = reader.getCurrentKey();
	    	String value = reader.getCurrentValue();
	    	LOG.info("Key: " + key + ", Value: " + value);
	    	pointFiles.add(value);
	    }
	    Configuration conf = context.getConfiguration();
	    runKmeans(pointFiles, conf, context);
	    LOG.info("Total iterations in master view: " + (System.currentTimeMillis() - startTime));
	  }
	  
	 private void broadcastCentroids( ArrTable<DoubleArray> cenTable) throws IOException{  
		 //broadcast centroids 
		  boolean isSuccess = false;
		  try {
			  isSuccess = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID());
		  } catch (Exception e) {
		      LOG.error("Fail to bcast.", e);
		  }
		  if (!isSuccess) {
		      throw new IOException("Fail to bcast");
		  }
	 }
	 
	 private void findNearestCenter(ArrTable<DoubleArray> cenTable, ArrTable<DoubleArray> previousCenTable, ArrayList<DoubleArray> dataPoints){
		double err=0;
		 for(DoubleArray aPoint: dataPoints){
				//for each data point, find the nearest centroid
				double minDist = 99999999;
				double tempDist = 0;
				int nearestPartitionID = -1;
				for(ArrPartition ap: previousCenTable.getPartitions()){
					DoubleArray aCentroid = (DoubleArray) ap.getArray();
                    for(int i=0; i<vectorSize; i++){
					  double d = Math.pow(aCentroid.getArray()[i] - aPoint.getArray()[i],2);
					  tempDist += d;
					}
					tempDist = Math.sqrt(tempDist);
					if(tempDist < minDist){
					  nearestPartitionID = ap.getPartitionID();
					  minDist = tempDist;
					}
					/* TODO - Write code here */					
				}
				err+=minDist;
				
				//for the certain data point, found the nearest centroid.
				// add the data to a new cenTable.
				double[] partial = new double[vectorSize+1];
				for(int j=0; j < vectorSize; j++){
					partial[j] = aPoint.getArray()[j];
				}
				partial[vectorSize]=1;
				
				if(cenTable.getPartition(nearestPartitionID) == null){
					ArrPartition<DoubleArray> tmpAp = new ArrPartition<DoubleArray>(nearestPartitionID, new DoubleArray(partial, 0, vectorSize+1));
					cenTable.addPartition(tmpAp);
					 
				}else{
					 ArrPartition<DoubleArray> apInCenTable = cenTable.getPartition(nearestPartitionID);
					 for(int i=0; i < vectorSize +1; i++){
						 apInCenTable.getArray().getArray()[i] += partial[i];
					 }
				}
			  }
		 System.out.println("Errors: "+err);
	 }
	
	  private void runKmeans(List<String> fileNames, Configuration conf, Context context) throws IOException {
		  // -----------------------------------------------------
		  // Load centroids
		  //for every partition in the centoid table, we will use the last element to store the number of points 
		  // which are clustered to the particular partitionID
		  ArrTable<DoubleArray> cenTable = new ArrTable<>(new DoubleArrPlus());
		  if (this.isMaster()) {
			  Utils.loadCentroids(cenTable, vectorSize, conf.get(KMeansConstants.CFILE), conf);
		  }
		  
		  System.out.println("After loading centroids");
		  Utils.printArrTable(cenTable);
		  
		  //broadcast centroids
		  broadcastCentroids(cenTable);
		  
		  //after broadcasting
		  System.out.println("After brodcasting centroids");
		  Utils.printArrTable(cenTable);
		  
		  //load data 
		  ArrayList<DoubleArray> dataPoints = Utils.loadData(fileNames, vectorSize, conf);
		  
		  ArrTable<DoubleArray> previousCenTable =  null;
		  //iterations
		  for(int iter=0; iter < iteration; iter++){
			  previousCenTable =  cenTable;
			  cenTable = new ArrTable<>(new DoubleArrPlus());
			  
			  System.out.println("Iteraton No."+iter);
			  
			  //compute new partial centroid table using previousCenTable and data points
			  findNearestCenter(cenTable, previousCenTable, dataPoints);
			  
			  //AllReduce; 
			  allreduce("main", "allreduce_"+iter, cenTable);
			  System.out.println("after allreduce");
			  Utils.printArrTable(cenTable);
			  
			  //we can calculate new centroids
			  updateCenters(cenTable);
			  
		  }
		  if(this.isMaster()){
			  outputCentroids(cenTable,  conf,   context);
		  }
	 }
	  
	  private void updateCenters(ArrTable<DoubleArray> cenTable){
		  for( ArrPartition<DoubleArray> partialCenTable: cenTable.getPartitions()){
			  double[] doubles = partialCenTable.getArray().getArray();
			  for(int i=0; i<vectorSize;i++){
			    doubles[i] = doubles[i] / doubles[vectorSize];
			  }
			  /* TODO - Write code here */
			  doubles[vectorSize] = 0;
		  }
		  System.out.println("after calculate new centroids");
		  Utils.printArrTable(cenTable);
	  }
	  
	  private void outputCentroids(ArrTable<DoubleArray>  cenTable,Configuration conf, Context context){
		  String output="";
		  for( ArrPartition<DoubleArray> ap: cenTable.getPartitions()){
			  double res[] = ap.getArray().getArray();
			  for(int i=0; i<vectorSize;i++)
				 output+= res[i]+"\t";
			  output+="\n";
		  }
			try {
				context.write(null, new Text(output));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  
	  

}
