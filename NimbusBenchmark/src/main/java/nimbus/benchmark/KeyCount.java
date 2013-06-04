package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import nimbus.client.DynamicMapClient;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.CacheType;

public class KeyCount extends Configured implements Tool {

	public static void getKVPair(String s, Pair<String, String> out) {
		String[] tokens = s.split("\\s", 2);
		out.setFirst(tokens[0]);
		out.setSecond(tokens[1]);
	}

	public static class NimbusKeyMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outkey = new Text(), outvalue = new Text();
		private Pair<String, String> pair = new Pair<String, String>();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			KeyCount.getKVPair(value.toString(), pair);

			outkey.set(pair.getFirst());
			outvalue.set(pair.getSecond());
			context.write(outkey, outvalue);
		}
	}

	public static class NimbusKeyReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
				break;
			}
		}
	}

	public static class NimbusKeyTesterMapper extends
			Mapper<Text, Text, Text, Text> {

		private DynamicMapClient client = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {

			try {
				client = new DynamicMapClient("test");
			} catch (CacheDoesNotExistException e) {

			}

			context.getCounter("Records", "Mismatch");
		}

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			long start = System.currentTimeMillis();
			if (client.get(key.toString()).equals(value.toString())) {
				context.getCounter("Records", "Match").increment(1);
			} else {

				context.getCounter("Records", "Mismatch").increment(1);
			}

			long finish = System.currentTimeMillis();

			context.getCounter("Records", "Queries").increment(1);
			context.getCounter("Records", "Query Time").increment(
					finish - start);
		}
	}

	private static final Logger LOG = Logger.getLogger(KeyCount.class);

	public int run(String[] args) throws Exception {

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Job job = new Job(getConf(), "Nimbus MR counter");
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusKeyMapper.class);
		job.setReducerClass(NimbusKeyReducer.class);
		job.setNumReduceTasks(1);

		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		NimbusMaster.getInstance().create("test", CacheType.DYNAMIC_MAP);

		DynamicMapClient client = new DynamicMapClient("test");

		LOG.info("Opening " + input);
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				FileSystem.get(getConf()).open(
						new Path(outputDir.toString() + "/part-r-00000"))));

		int i = 0;
		String s;
		while ((s = rdr.readLine()) != null) {
			client.put(s.split("\t")[0], s.split("\t")[1]);

			if (++i % 1000000 == 0) {
				LOG.info("Read " + i + " records");
			}
		}

		client.flush();

		LOG.info("Closed reader.  Read " + i + " records");
		rdr.close();

		client.disconnect();

		job = new Job(getConf(), "Nimbus MR counter");
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusKeyTesterMapper.class);
		job.setNumReduceTasks(0);

		KeyValueTextInputFormat.setInputPaths(job, new Path(args[0]));
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputFormatClass(NullOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new KeyCount(), args));
	}
}
