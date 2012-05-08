import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		FileSplit fs = (FileSplit)context.getInputSplit();
		String filename = fs.getPath().getName();		
		Text outVal = new Text(filename + "@" + key.toString());

		while (tokenizer.hasMoreTokens()) {
			Text word = new Text(tokenizer.nextToken());
			context.write(word, outVal);
		}
	}
	
}
