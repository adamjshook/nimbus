package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import nimbus.client.MapSetClient;
import nimbus.mapreduce.lib.input.NimbusMapSetInputFormat;
import nimbus.mapreduce.lib.output.NimbusMapSetOutputFormat;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.FailedToCreateCacheException;
import nimbus.master.NimbusMaster;
import nimbus.server.CacheType;
import nimbus.usermentions.HBaseUserMentions;
import nimbus.usermentions.UserMentions;

public class NimbusBenchmark extends Configured implements Tool {

	public static class NimbusScanMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class NimbusIngestMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private String[] tokens;
		private Text outkey = new Text(), outvalue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			tokens = value.toString().split("\t");
			outkey.set(tokens[0]);
			outvalue.set(tokens[1]);
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
		private MapSetClient client = null;
		private String[] tokens = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			sampleRate = Float.parseFloat(context.getConfiguration().get(
					NIMBUS_SAMPLE_MAPPER_RATE));

			try {
				client = new MapSetClient(context.getConfiguration().get(
						NIMBUS_SAMPLE_MAPPER_CACHE_NAME));
			} catch (CacheDoesNotExistException e) {
				throw new IOException(e);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			tokens = value.toString().split("\t");
			if (rndm.nextFloat() < sampleRate) {
				if (!client.get(tokens[0]).equals(tokens[1])) {
					context.getCounter("Records", "Mismatch").increment(1);
				}
			}
		}
	}

	private static final Logger LOG = Logger.getLogger(NimbusBenchmark.class);

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: <input> <cachename> [nimbus|hbase]");
			return 1;
		}

		Path input = new Path(args[0]);
		String cacheName = args[1];
		String type = args[2];

		if (type.equalsIgnoreCase("nimbus")) {
			return doNimbusBenchmark(input, cacheName);
		} else {
			return 0;// doHBaseBenchmark(input, cacheName);
		}
	}

	private int doNimbusBenchmark(Path input, String cacheName)
			throws Exception {

		serialIngest(input, cacheName);
		serialScan(cacheName);
		mapReduceScan(cacheName);
		sampleScan(input, cacheName, .0f);
		sampleScan(input, cacheName, .01f);
		sampleScan(input, cacheName, .05f);
		sampleScan(input, cacheName, .10f);

		NimbusMaster.getInstance().destroy(cacheName);

		mapReduceIngest(input, cacheName);

		return 0;
	}

	private void serialIngest(Path input, String cacheName) throws Exception {

		LOG.info("Executing serial ingest of data from " + input + " into "
				+ cacheName);

		long start = System.currentTimeMillis();

		FileSystem fs = FileSystem.get(getConf());

		NimbusMaster.getInstance().create(cacheName, CacheType.MAPSET);

		MapSetClient client = new MapSetClient(cacheName);
		FileStatus[] paths = fs.listStatus(input);

		String s;
		String[] tokens;
		for (FileStatus file : paths) {
			LOG.info("Opening " + file.getPath());
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					fs.open(file.getPath())));

			int i = 0;
			while ((s = rdr.readLine()) != null) {
				tokens = s.split("\\s");
				client.add(tokens[0], tokens[1]);

				if (++i % 1000000 == 0) {
					LOG.info("Read " + i + " records");
				}
			}

			LOG.info("Closed reader.  Read " + i + " records");
			rdr.close();
		}

		client.disconnect();
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
	}

	private void serialScan(String cacheName) throws Exception {

		LOG.info("Executing serial scan of data from " + cacheName);

		long start = System.currentTimeMillis();

		MapSetClient client = new MapSetClient(cacheName);

		int i = 0;
		for (@SuppressWarnings("unused")
		Entry<String, String> entries : client) {
			if (++i % 1000000 == 0) {
				LOG.info("Read " + i + " records");
			}
		}

		client.disconnect();

		LOG.info("Closed reader.  Read " + i + " records");
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
	}

	private void mapReduceScan(String cacheName) throws Exception {

		LOG.info("Executing full MR scan of data from " + cacheName);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "Nimbus MR Scan");
		job.setJarByClass(getClass());

		job.setMapperClass(NimbusScanMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(NimbusMapSetInputFormat.class);
		NimbusMapSetInputFormat.setCacheName(job, cacheName);

		job.setOutputFormatClass(NullOutputFormat.class);

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
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
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(NimbusMapSetOutputFormat.class);
		NimbusMapSetOutputFormat.setCacheName(job, cacheName);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
	}

	private void doHBaseIngest(Configuration conf, Path input)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Mention Ingest-HBase");
		job.setJarByClass(NimbusBenchmark.class);

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new NimbusBenchmark(), args);
	}
}
