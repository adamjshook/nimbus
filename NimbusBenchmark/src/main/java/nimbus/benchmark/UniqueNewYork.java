package nimbus.benchmark;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class UniqueNewYork extends Configured implements Tool {

	public static class KeyValueSplitter extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outkey = new Text(), outvalue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] kv = value.toString().split("\\s");

			outkey.set(kv[0]);
			for (int i = 1; i < kv.length; ++i) {
				if (kv[i].length() != 0) {
					if (kv[i].length() > 90) {
						outvalue.set(kv[i].substring(0, 90));
					} else {
						while (kv[i].length() < 90) {
							kv[i] = kv[i] + "A";
						}
						outvalue.set(kv[i]);
					}
					break;
				}
			}
			if (outkey.toString().length() == 10) {
				context.write(outkey, outvalue);
			}
		}
	}

	public static class KeyValueUniquer extends Reducer<Text, Text, Text, Text> {

		private long bytes = 0;
		private static final long MAX_BYTES = 6L*1024L*1024L*1024L;
		private Text value;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (bytes <= MAX_BYTES) {
				bytes += key.getBytes().length;
				value = values.iterator().next();
				bytes += value.toString().getBytes().length;
				context.write(key, value);
			}
		}
	}

	private static final Logger LOG = Logger.getLogger(UniqueNewYork.class);

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: <input> <output>");
			return 1;
		}

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		getConf().set("fs.default.name", "hdfs://nimbus-1:9000");

		Job job = new Job(getConf(), "Unique New York");
		job.setJarByClass(getClass());

		job.setMapperClass(KeyValueSplitter.class);
		job.setReducerClass(KeyValueUniquer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new UniqueNewYork(), args);
	}
}
