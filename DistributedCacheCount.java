package wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheCount extends Configured implements Tool {

	/**
	 * Mapper class, uses distributed cache file to read patterns from small list
	 * and find them in input file and emit
	 * @author dvarik
	 *
	 */
	public static class DistribMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Set<String> patternWords = new HashSet<String>();

		/**
		 * Setup function to read distributed cache file of word list and save
		 * pattern words locally into set.
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String patternFile = context.getLocalCacheFiles()[0].toString();
			try {
				BufferedReader fis = new BufferedReader(new FileReader(new File(patternFile)));
				String line;
				while ((line = fis.readLine()) != null) {
					String[] words = line.split(" ");
					for (String word : words)
						patternWords.add(word);
				}
				fis.close();
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + patternFile + "' : " + ioe.getMessage());
			}
			super.setup(context);
		}

		/**
		 * Mapper function, iterates through big input file words,
		 * checks if they are present in word list set, and if so then emits with count of one. 
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if (patternWords.contains(token)) {
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}

	/**
	 * Reducer class, takes mapper output and sums count of each word.
	 * @author dvarik
	 *
	 */
	public static class DistribReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new DistributedCacheCount(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "distributedcachecount");
		job.addCacheFile(new Path(args[2]).toUri());
		job.setJarByClass(DistributedCacheCount.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(DistribMapper.class);
		job.setCombinerClass(DistribReducer.class);
		job.setReducerClass(DistribReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
