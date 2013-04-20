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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

public class HBaseBenchmarker extends Configured {

	private static final Logger LOG = Logger.getLogger(HBaseBenchmarker.class);
	private static final byte[] COLUMN_FAMILY = "c".getBytes();
	private static final byte[] EMPTY_BYTES = "".getBytes();

	public static class HBaseScanMapper extends
			TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static class HBaseIngestMapper extends
			Mapper<LongWritable, Text, NullWritable, Put> {

		private NullWritable outkey = NullWritable.get();
		private Put outvalue = null;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			outvalue = new Put(value.toString()
					.getBytes());
			outvalue.add(COLUMN_FAMILY, EMPTY_BYTES, EMPTY_BYTES);
			context.write(outkey, outvalue);
		}
	}

	public static class HBaseSampleMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public static final String HBASE_SAMPLE_MAPPER_RATE = "hbase.sample.mapper.rate";
		public static final String HBASE_SAMPLE_MAPPER_CACHE_NAME = "hbase.sample.mapper.cache.name";

		public static void setSampleRate(Job job, float rate) {
			job.getConfiguration().set(HBASE_SAMPLE_MAPPER_RATE,
					Float.toString(rate));
		}

		public static void setCacheName(Job job, String cacheName) {
			job.getConfiguration().set(HBASE_SAMPLE_MAPPER_CACHE_NAME,
					cacheName);
		}

		private float sampleRate = 0.0f;
		private Random rndm = new Random(0);
		private HTable client = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			sampleRate = Float.parseFloat(context.getConfiguration().get(
					HBASE_SAMPLE_MAPPER_RATE));

			client = new HTable(context.getConfiguration(), context
					.getConfiguration().get(HBASE_SAMPLE_MAPPER_CACHE_NAME)
					.getBytes());

			context.getCounter("Records", "Mismatch");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rndm.nextFloat() < sampleRate) {
				long start = System.currentTimeMillis();
				if (!client.exists(new Get(value.toString().getBytes()))) {
					context.getCounter("Records", "Mismatch").increment(1);
				}
				long finish = System.currentTimeMillis();

				context.getCounter("Records", "Queries").increment(1);
				context.getCounter("Records", "Query Time").increment(
						finish - start);
			}
		}
	}

	public int run(Path input, String tableName, byte[][] splits)
			throws Exception {

		setConf(HBaseConfiguration.create(getConf()));

		serialIngest(input, tableName, splits);
		serialScan(tableName);
		mapReduceScan(tableName);
		sampleScan(input, tableName, .0f);
		sampleScan(input, tableName, .01f);
		sampleScan(input, tableName, .05f);
		sampleScan(input, tableName, .10f);
		sampleScan(input, tableName, .25f);
		sampleScan(input, tableName, .50f);

		LOG.info("Destroying " + tableName);

		HBaseAdmin admin = new HBaseAdmin(getConf());
		admin.disableTable(tableName.getBytes());
		admin.deleteTable(tableName);

		LOG.info("Sleeping for 30s");
		Thread.sleep(30000);

		mapReduceIngest(input, tableName, splits);

		LOG.info("Destroying " + tableName);
		admin.disableTable(tableName.getBytes());
		admin.deleteTable(tableName);
		return 0;
	}

	private void serialIngest(Path input, String tableName, byte[][] splits)
			throws Exception {

		LOG.info("Executing serial ingest of data from " + input + " into "
				+ tableName);

		long start = System.currentTimeMillis();

		FileSystem fs = FileSystem.get(getConf());

		createTable(tableName, splits);

		FileStatus[] paths = fs.listStatus(input, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return !(path.getName().startsWith("_") || path.getName()
						.startsWith("."));
			}
		});

		HTable table = new HTable(getConf(), tableName.getBytes());
		table.setAutoFlush(false);

		String s;
		for (FileStatus file : paths) {
			if (!file.isDir()) {
				LOG.info("Opening " + file.getPath());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(file.getPath())));

				Put outvalue = null;
				int i = 0;
				while ((s = rdr.readLine()) != null) {
					outvalue = new Put(s.getBytes());
					outvalue.add(COLUMN_FAMILY, EMPTY_BYTES, EMPTY_BYTES);

					table.put(outvalue);

					if (++i % 1000000 == 0) {
						LOG.info("Read " + i + " records");
					}
				}

				LOG.info("Closed reader.  Read " + i + " records");
				rdr.close();
			} else {
				LOG.warn(file.getPath() + " is a directory. Ignoring");
			}
		}

		table.close();
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for ingest.");
	}

	private void serialScan(String tableName) throws Exception {

		LOG.info("Executing serial scan of data from " + tableName);

		long start = System.currentTimeMillis();

		HTable table = new HTable(getConf(), tableName.getBytes());
		LOG.info("Default scanner caching: " + table.getScannerCaching());
		Scan scan = new Scan();
		scan.setCaching(671088);
		scan.setBatch(Integer.MAX_VALUE);
		ResultScanner scanner = table.getScanner(scan);

		int i = 0;
		for (@SuppressWarnings("unused")
		Result entry : scanner) {
			if (++i % 1000000 == 0) {
				LOG.info("Read " + i + " records");
			}
		}

		table.close();

		LOG.info("Closed reader.  Read " + i + " records");
		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for scan.");
	}

	private void mapReduceScan(String tableName) throws Exception {

		LOG.info("Executing full MR scan of data from " + tableName);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "HBase MR Scan");
		job.setJarByClass(getClass());

		job.setMapperClass(HBaseScanMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
	
		Scan scan = new Scan();
                scan.setCaching(5000);
                scan.setBatch(Integer.MAX_VALUE);

		TableMapReduceUtil.initTableMapperJob(tableName.getBytes(), scan, HBaseScanMapper.class, Text.class, Text.class, job);

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for MR scan.");
	}

	private void sampleScan(Path input, String tableName, float sampleRate)
			throws Exception {
		LOG.info("Executing sample MR scan of data from " + tableName
				+ ".  Sample rate " + sampleRate);

		long start = System.currentTimeMillis();

		Job job = new Job(getConf(), "HBase MR Sample " + sampleRate);
		job.setJarByClass(getClass());

		job.setMapperClass(HBaseSampleMapper.class);
		HBaseSampleMapper.setCacheName(job, tableName);
		HBaseSampleMapper.setSampleRate(job, sampleRate);

		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, input);

		job.setOutputFormatClass(NullOutputFormat.class);

		job.waitForCompletion(true);

		long finish = System.currentTimeMillis();

		LOG.info("Took " + (finish - start) + " ms for scanning " + tableName
				+ " at " + sampleRate);
	}

	private void mapReduceIngest(Path input, String tableName, byte[][] splits)
			throws Exception {
		LOG.info("Executing MR ingest of data from " + tableName);

		long start = System.currentTimeMillis();

		createTable(tableName, splits);

		Job job = new Job(getConf(), "HBase MR Ingest");
		job.setJarByClass(getClass());

		job.setMapperClass(HBaseIngestMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);

		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);

		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();
		LOG.info("Took " + (finish - start) + " ms for MR ingest.");
	}

	private void createTable(String tableName, byte[][] splits)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConf());
		HTableDescriptor desc = new HTableDescriptor(tableName.getBytes());
		HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY);
		family.setBloomFilterType(BloomType.ROW);
		//family.setBlocksize(16777216);
		family.setCompactionCompressionType(Algorithm.NONE);
		//family.setInMemory(true);
		family.setMaxVersions(1);

		desc.addFamily(family);
		admin.createTable(desc, splits);
	}
}

