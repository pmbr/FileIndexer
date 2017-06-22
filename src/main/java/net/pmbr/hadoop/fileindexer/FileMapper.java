package net.pmbr.hadoop.fileindexer;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileMapper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * 
	 * Hadoop can reuse an instance of an mapper to process different inputs,
	 * in this case, it can reuse an mapper to process different files.
	 * 
	 * For this reason, it is a good approach to have instance variables
	 * which can be managed as 'stateless' variables and be reused across
	 * executions of method 'map' and avoid to instanciate then for each
	 * execution time.
	 * 
	 */
	private Text filename = new Text();
	private Text word = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = ((FileSplit) context.getInputSplit());
		this.filename.set(fileSplit.getPath().getName());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		for (String token : StringUtils.split(value.toString())) {
			word.set(token);
			context.write(word, filename);
		}
	}

}
