/**
 * 
 */
package com.saroj.demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author sarojrout
 *
 */
public class MaxTemperatureDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JobConf conf=new JobConf();
		Job job;
		try{
			job=Job.getInstance();
			job.setJobName("temp measure");
			job.setMapperClass(MaxTemperatureMapper.class);
			job.setReducerClass(MaxtemperatureReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job,new Path("/Users/sarojrout/sample.txt"));
	        FileOutputFormat.setOutputPath(job,new Path("/Users/sarojrout/hadoopLogs"));
	        try {
	            job.waitForCompletion(true);
	            System.out.println("extracted");
	        } catch (ClassNotFoundException | IOException | InterruptedException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
		}catch(Exception ex){
			ex.printStackTrace();
		}

	}

}
