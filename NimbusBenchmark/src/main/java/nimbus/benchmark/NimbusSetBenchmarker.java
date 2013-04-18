package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import nimbus.client.DynamicSetClient;
import nimbus.mapreduce.DynamicSetPartitioner;
import nimbus.mapreduce.lib.input.DynamicSetInputFormat;
import nimbus.mapreduce.lib.output.DynamicSetOutputFormat;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.CacheType;

public class NimbusSetBenchmarker extends Configured {

	public static class NimbusScanMapper extends
			Mapper<Text, Object, Text, Text> {

		@Override
		protected void map(Text key, Object value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class NimbusIngestMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outkey = new Text();
		private NullWritable outvalue = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			outkey.set(NimbusBenchmark.makeNimbusSafe(value.toString()));
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
		private DynamicSetClient client = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			sampleRate = Float.parseFloat(context.getConfiguration().get(
					NIMBUS_SAMPLE_MAPPER_RATE));

			try {
				client = new DynamicSetClient(context.getConfiguration().get(
						NIMBUS_SAMPLE_MAPPER_CACHE_NAME));
			} catch (CacheDoesNotExistException e) {

			}

			context.getCounter("Records", "Mismatch");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rndm.nextFloat() < sampleRate) {
				long start = System.currentTimeMillis();
				if (!client.contains(NimbusBenchmark.makeNimbusSafe(value
						.toString()))) {
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
			.getLogger(NimbusSetBenchmarker.class);

	public int run(Path input, String cacheName) throws Exception {

		serialIngest(input, cacheName);
		serialScan(cacheName);
		mapReduceScan(cacheName);
		sampleScan(input, cacheName, .0f);
		sampleScan(input, cacheName, .01f);
		sampleScan(input, cacheName, .05f);
		sampleScan(input, cacheName, .10f);

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

		NimbusMaster.getInstance().create(cacheName, CacheType.DYNAMIC_SET);

		DynamicSetClient client = new DynamicSetClient(cacheName);
		FileStatus[] paths = fs.listStatus(input, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return !(path.getName().startsWith("_") || path.getName()
						.startsWith("."));
			}
		});

		String s;
		for (FileStatus file : paths) {
			if (!file.isDir()) {
				LOG.info("Opening " + file.getPath());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(file.getPath())));

				int i = 0;
				while ((s = rdr.readLine()) != null) {
					client.add(NimbusBenchmark.makeNimbusSafe(s));

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

		DynamicSetClient client = new DynamicSetClient(cacheName);

		LOG.info("Beginning scan");
		int i = 0;
		for (@SuppressWarnings("unused")
		String entry : client) {
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

		job.setMapperClass(NimbusScanMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(DynamicSetInputFormat.class);
		DynamicSetInputFormat.setCacheName(job, cacheName);

		job.setOutputFormatClass(NullOutputFormat.class);

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

		job.setMapperClass(NimbusIngestMapper.class);
		job.setPartitionerClass(DynamicSetPartitioner.class);

		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(DynamicSetOutputFormat.class);
		DynamicSetOutputFormat.setCacheName(job, cacheName);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();
		LOG.info("Took " + (finish - start) + " ms for MR ingest.");
	}
}
