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
		int res = ToolRunner.run(new Configuration(), new FileIndexerJob(), args);

		System.exit(res);
	}

	public Configuration getConf() {
		return super.getConf();
	}

	public int run(String[] arg0) throws Exception {
		
		Path input = new Path("indexer/input");
		Path output = new Path("indexer/output");

		Configuration config = getConf();
		
		Job job = new Job(config);
		
		job.setJarByClass(FileIndexerJob.class);
		job.setMapperClass(FileMapper.class);
		job.setReducerClass(FileReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		if (job.waitForCompletion(true)) {
			return 0;
		}

		return 1;
	}
}
