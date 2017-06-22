package net.pmbr.hadoop.fileindexer;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text filename;
	private Text word = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		FileSplit fileSplit = ((FileSplit) context.getInputSplit());

		this.filename = new Text(fileSplit.getPath().getName());

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		for (String token : StringUtils.split(value.toString())) {
			word.set(token);
			context.write(word, filename);
		}
	}

}
