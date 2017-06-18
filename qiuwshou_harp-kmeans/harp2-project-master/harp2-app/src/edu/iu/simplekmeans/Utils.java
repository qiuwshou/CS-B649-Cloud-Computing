package edu.iu.simplekmeans;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.trans.DoubleArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    //calculate Euclidean distance.
    static double calcEucDistSquare(DoubleArray aPoint, DoubleArray otherPoint, int vectorSize){
        double dist=0;
        for(int i=0; i < vectorSize; i++){
            dist += Math.pow(aPoint.getArray()[i]-otherPoint.getArray()[i],2);
        }
        return Math.sqrt(dist);
    }

    //load centroids from HDFS
    static void loadCentroids(ArrTable<DoubleArray> cenTable, int vectorSize, String cFileName, Configuration configuration) throws

        IOException {
        Path cPath = new Path(cFileName);
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream in = fs.open(cPath);
        BufferedReader br = new BufferedReader( new InputStreamReader(in));
        String line="";
        String[] vector=null;
        int partitionId=0;
        while((line = br.readLine()) != null){
            vector = line.split("\\s+");
            if(vector.length != vectorSize){
                System.out.println("Errors while loading centroids .");
                System.exit(-1);
            }else{
                double[] aCen = new double[vectorSize+1];

                for(int i=0; i<vectorSize; i++){
                    aCen[i] = Double.parseDouble(vector[i]);
                }
                aCen[vectorSize]=0;
                ArrPartition<DoubleArray>
                    ap = new ArrPartition<DoubleArray>(partitionId, new DoubleArray(aCen, 0, vectorSize + 1));
                cenTable.addPartition(ap);
                partitionId++;
            }
        }
    }
    //load data form HDFS
    static ArrayList<DoubleArray> loadData(List<String> fileNames, int vectorSize, Configuration conf) throws IOException{
        ArrayList<DoubleArray> data = new  ArrayList<DoubleArray> ();
        for(String filename: fileNames){
            FileSystem fs = FileSystem.get(conf);
            Path dPath = new Path(filename);
            FSDataInputStream in = fs.open(dPath);
            BufferedReader br = new BufferedReader( new InputStreamReader(in));
            String line="";
            String[] vector=null;
            while((line = br.readLine()) != null){
                vector = line.split("\\s+");

                if(vector.length != vectorSize){
                    System.out.println("Errors while loading data.");
                    System.exit(-1);
                }else{
                    double[] aDataPoint = new double[vectorSize];

                    for(int i=0; i<vectorSize; i++){
                        aDataPoint[i] = Double.parseDouble(vector[i]);
                    }
                    DoubleArray da = new DoubleArray(aDataPoint, 0, vectorSize);
                    data.add(da);
                }
            }
        }
        return data;
    }

    //for testing
    static void printArrTable(ArrTable<DoubleArray> dataTable){
        for( ArrPartition<DoubleArray> ap: dataTable.getPartitions()){

            double res[] = ap.getArray().getArray();
            System.out.print("ID: "+ap.getPartitionID() + ":");
            for(int i=0; i<res.length;i++)
                System.out.print(res[i]+"\t");
            System.out.println();
        }
    }
}
