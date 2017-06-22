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
	 * Hadoop can reuse an instance of an mapper to process different inputs, in this case, it can reuse an mapper to process different files.
	 * 
	 * For this reason, it is a good approach to have instance variables which can be managed as 'stateless' variables and be reused across
	 * executions of method 'map' and avoid to instanciate then for each execution time.
	 * 
	 */
	private Text filename = new Text();
	private Text word = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//This extract from context the name of file to be processed by the mapper
		FileSplit fileSplit = ((FileSplit) context.getInputSplit());

		//Filename is set to instance variable to be used by map method
		this.filename.set(fileSplit.getPath().getName());
	}

	@Override
	protected void map(LongWritable key, Text fileContent, Context context) throws IOException, InterruptedException {
		/*
		 * fileContent input contains entire text file content
		 * Splitting the content gives a list of all words from that file
		 * Each word is added to map/reduce context associated with its respective file
		 * During reduce phase this information will be retrieved from context and passed to reducer
		 * Basically context is used here as sort of shared memory used by Hadoop to transfer data from mappers to reducers 
		 */  
		for (String token : StringUtils.split(fileContent.toString())) {
			word.set(token);
			context.write(word, filename);
		}
	}

}
