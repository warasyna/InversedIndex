import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, Text, Text, Text> {
	private static final String SEP = ",";

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		StringBuilder sb    = new StringBuilder();
		boolean       first = true;

		for (Text value : values) {
			if (!first) sb.append(SEP);
			else first = false;
			
			sb.append(value.toString());
		}

		context.write(key, new Text(sb.toString()));
	}
}