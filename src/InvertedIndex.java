import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		
		//WARNING: this function is for debug. NEVER use in performance
		delete(new File(args[1]));
//		Formatter fr = new Formatter();
//		String outpath = args[1] + fr.format("%1$tm%1$td%1$tH%1$tM%1$tS", new Date());
		
		Job job = new Job(conf, "InvertedIndex: input=" + args[0] + " output=" + args[1]);
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		System.out.println(success);
	}
	
	static private void delete(File f) {
		if (!f.exists()) return;
		
		if (f.isFile()) f.delete();
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			
			for(int i = 0; i < files.length; i++) 
				delete(files[i]);
			
			f.delete();
		}
	}

}
