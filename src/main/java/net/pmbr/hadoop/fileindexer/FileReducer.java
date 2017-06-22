package net.pmbr.hadoop.fileindexer;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer<Text, Text, Text, Text> {

	private Text filenames = new Text();

	public void reduce(Text word, Iterable<Text> files, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		HashSet<String> uniqueFiles = new HashSet<String>();

		for (Text file : files) {
			uniqueFiles.add(file.toString());
		}

		filenames.set(new Text(StringUtils.join(uniqueFiles, ",")));

		context.write(word, filenames);

	}

}
