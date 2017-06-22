package net.pmbr.hadoop.fileindexer;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * 
	 * Hadoop can reuse an instance of a reducer to process different data,
	 * in this case, it can reuse a reducer to process different words.
	 * 
	 * For this reason, it is a good approach to have instance variables
	 * which can be managed as 'stateless' variables and be reused across
	 * executions of method 'map' and avoid to instanciate then for each
	 * execution time.
	 * 
	 */
	private Text filenames = new Text();

	public void reduce(Text word, Iterable<Text> files, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		//File names are added to to a list 
		HashSet<String> uniqueFiles = new HashSet<String>();
		for (Text file : files) {
			uniqueFiles.add(file.toString());
		}

		//File names are joined on comma-separated list
		filenames.set(new Text(StringUtils.join(uniqueFiles, ",")));

		//Pair of word and name of files where it can be found are added to context
		//This will be used by Hadoop to create output file containing index of words and files
		context.write(word, filenames);

	}

}
