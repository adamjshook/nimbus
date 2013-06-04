package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import nimbus.client.DynamicMapClient;
import nimbus.mapreduce.NimbusHashPartitioner;
import nimbus.mapreduce.lib.input.DynamicMapInputFormat;
import nimbus.mapreduce.lib.output.DynamicMapOutputFormat;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.CacheType;

public class NimbusMapBenchmarker extends Configured {

	public static void getKVPair(String s, Pair<String, String> out) {
		String[] tokens = s.split("\\s", 2);
		out.setFirst(tokens[0]);
		out.setSecond(tokens[1]);
	}

	public static class NimbusMapScanMapper extends
			Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class NimbusMapIngestMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outkey = new Text(), outvalue = new Text();
		private Pair<String, String> pair = new Pair<String, String>();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			NimbusMapBenchmarker.getKVPair(value.toString(), pair);

			outkey.set(pair.getFirst());
			outvalue.set(pair.getSecond());
			context.write(outkey, outvalue);
		}
	}

	public static class NimbusSampleMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public static final String NIMBUS_SAMPLE_MAPPER_RATE = "nimbus.sample.mapper.rate";
		public static final String NIMBUS_SAMPLE_MAPPER_CACHE_NAME = "nimbus.sample.mapper.cache.name";

		public static void setSampleRate(Job job, float rate) {
			job.getConfiguration().set(NIMBUS_SAMPLE_MAPPER_RATE,
					Float.toString(rate));
		}

		public static void setCacheName(Job job, String cacheName) {
			job.getConfiguration().set(NIMBUS_SAMPLE_MAPPER_CACHE_NAME,
					cacheName);
		}

		private float sampleRate = 0.0f;
		private Random rndm = new Random(0);
		private DynamicMapClient client = null;
		private Pair<String, String> pair = new Pair<String, String>();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			sampleRate = Float.parseFloat(context.getConfiguration().get(
					NIMBUS_SAMPLE_MAPPER_RATE));

			try {
				client = new DynamicMapClient(context.getConfiguration().get(
						NIMBUS_SAMPLE_MAPPER_CACHE_NAME));
			} catch (CacheDoesNotExistException e) {

			}

			context.getCounter("Records", "Mismatch");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rndm.nextFloat() < sampleRate) {

				NimbusMapBenchmarker.getKVPair(value.toString(), pair);

				long start = System.currentTimeMillis();
				if (!client.get(pair.getFirst()).equals(pair.getSecond())) {
					context.getCounter("Records", "Mismatch").increment(1);
				}

				long finish = System.currentTimeMillis();

				context.getCounter("Records", "Queries").increment(1);
				context.getCounter("Records", "Query Time").increment(
						finish - start);
			}
		}
	}

	private static final Logger LOG = Logger
			.getLogger(NimbusMapBenchmarker.class);

	public int run(Path input, String cacheName) throws Exception {

		serialIngest(input, cacheName);
		//serialScan(cacheName);
		//mapReduceScan(cacheName);
		//sampleScan(input, cacheName, .0f);
		//sampleScan(input, cacheName, .01f);
		//sampleScan(input, cacheName, .05f);
		//sampleScan(input, cacheName, .10f);
		//sampleScan(input, cacheName, .25f);
		sampleScan(input, cacheName, 2.0f);

		LOG.info("Destroying " + cacheName);
		NimbusMaster.getInstance().destroy(cacheName);
		LOG.info("Sleeping for 30s");
		Thread.sleep(30000);

		mapReduceIngest(input, cacheName);

		LOG.info("Destroying " + cacheName);
		NimbusMaster.getInstance().destroy(cacheName);
		return 0;
	}

	private void serialIngest(Path input, String cacheName) throws Exception {

		LOG.info("Executing serial ingest of data from " + input + " into "
				+ cacheName);

		long start = System.currentTimeMillis();

		FileSystem fs = FileSystem.get(getConf());

		NimbusMaster.getInstance().create(cacheName, CacheType.DYNAMIC_MAP);

		DynamicMapClient client = new DynamicMapClient(cacheName);
		FileStatus[] paths = fs.listStatus(input, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return !(path.getName().startsWith("_") || path.getName()
						.startsWith("."));
			}
		});

		String s;
		Pair<String, String> pair = new Pair<String, String>();
		for (FileStatus file : paths) {
			if (!file.isDir()) {
				LOG.info("Opening " + file.getPath());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(file.getPath())));

				int i = 0;
				while ((s = rdr.readLine()) != null) {
					NimbusMapBenchmarker.getKVPair(s, pair);
					client.put(pair.getFirst(), pair.getSecond());

					if (++i % 1000000 == 0) {
						LOG.info("Read " + i + " records");
					}
				}

				client.flush();

				LOG.info("Closed reader.  Read " + i + " records");
				rdr.close();
			} else {
				LOG.warn(file.getPath() + " is a directory. Ignoring");
			}
		}

		client.disconnect();
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
	}

	private void serialScan(String cacheName) throws Exception {

		LOG.info("Executing serial scan of data from " + cacheName);

		long start = System.currentTimeMillis();

		DynamicMapClient client = new DynamicMapClient(cacheName);

		LOG.info("Beginning scan");
		int i = 0;
		for (@SuppressWarnings("unused")
		Entry<String, String> entry : client) {
			if (++i % 1000000 == 0) {
				LOG.info("Read " + i + " records");
			}
		}

		client.disconnect();

		LOG.info("Closed reader.  Read " + i + " records");
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for scan.");
	}

	private void mapReduceScan(String cacheName) throws Exception {

		LOG.info("Executing full MR scan of data from " + cacheName);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "Nimbus MR Scan");
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusMapScanMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(DynamicMapInputFormat.class);
		DynamicMapInputFormat.setCacheName(job, cacheName);

		job.setOutputFormatClass(NullOutputFormat.class);

		/*job.getConfiguration().set("mapred.map.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");
		job.getConfiguration().set("mapred.reduce.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");*/

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for MR scan.");
	}

	private void sampleScan(Path input, String cacheName, float sampleRate)
			throws Exception {
		LOG.info("Executing sample MR scan of data from " + cacheName
				+ ".  Sample rate " + sampleRate);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "Nimbus MR Sample " + sampleRate);
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusSampleMapper.class);
		NimbusSampleMapper.setCacheName(job, cacheName);
		NimbusSampleMapper.setSampleRate(job, sampleRate);

		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, input);

		job.setOutputFormatClass(NullOutputFormat.class);

		/*job.getConfiguration().set("mapred.map.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");
		job.getConfiguration().set("mapred.reduce.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");*/

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for scanning " + cacheName
				+ " at " + sampleRate);
	}

	private void mapReduceIngest(Path input, String cacheName) throws Exception {
		LOG.info("Executing MR ingest of data from " + cacheName);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "Nimbus MR Ingest");
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusMapIngestMapper.class);
		job.setPartitionerClass(NimbusHashPartitioner.class);

		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(DynamicMapOutputFormat.class);
		DynamicMapOutputFormat.setCacheName(job, cacheName);

		/*job.getConfiguration().set("mapred.map.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");
		job.getConfiguration().set("mapred.reduce.child.env",
				"NIMBUS_HOME=/home/ajshook/nimbus/Nimbus");*/

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();
		LOG.info("Took " + (finish - start) + " ms for MR ingest.");
	}
}
