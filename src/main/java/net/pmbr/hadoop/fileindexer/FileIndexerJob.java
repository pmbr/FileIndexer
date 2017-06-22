package net.pmbr.hadoop.fileindexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileIndexerJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		//Hadoop jobs for mapper and reducer are triggered using Hadoop utility classes
		int res = ToolRunner.run(new Configuration(), new FileIndexerJob(), args);

		System.exit(res);
	}

	public Configuration getConf() {
		return super.getConf();
	}

	public int run(String[] arg0) throws Exception {
		//Create a Hadoop job to run mapper and reducer
		Job job = Job.getInstance(getConf());
		
		//Set the jar file which contains mapper and reducer implementation using another class as reference
		//For this case is is used the class that is creating and triggering the job
		job.setJarByClass(FileIndexerJob.class);
		
		//Set the mapper class implementation
		job.setMapperClass(FileMapper.class);
		
		//Set the reducer class implementation
		job.setReducerClass(FileReducer.class);
		
		/*
		 * Set classes for key and value output of mapper class
		 * 
		 * For this implementation example both are Text: word and filename
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Create and reference to source folder of input files inside HDFS
		Path input = new Path("indexer/input");
		FileInputFormat.setInputPaths(job, input);
		
		//Create and reference to target folder of output files inside HDFS
		Path output = new Path("indexer/output");
		FileOutputFormat.setOutputPath(job, output);
		
		if (job.waitForCompletion(true)) {
			//Job finished with success
			return 0;
		}

		//Job failed
		return 1;
	}
}
