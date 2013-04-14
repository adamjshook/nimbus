package nimbus.benchmark;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import nimbus.client.MapSetClient;
import nimbus.mapreduce.lib.output.NimbusMapSetOutputFormat;
import nimbus.master.CacheDoesNotExistException;
import nimbus.usermentions.HBaseUserMentions;
import nimbus.usermentions.UserMentions;

public class NimbusMapSetTest extends Configured implements Tool {

	public static class TweetGraphBuilderTheFirst extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outkey = new Text(), outvalue = new Text();

		private UserMentions mentions = null;
		private MultipleOutputs<Text, Text> mos = null;
		private long connections = 0, containsQueries = 0, deadend = 0,
				numConnected = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				mentions = UserMentions.get(context.getConfiguration());
				if (mentions == null) {
					throw new IOException("Mentions is null");
				}
				mos = new MultipleOutputs<Text, Text>(context);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] userPair = value.toString().toLowerCase().split(" ");

			if (userPair[0].equals(userPair[1])) {
				// throw away pairs that are equal to one another
				return;
			}

			outkey.set(value.toString().toLowerCase());

			long start = System.currentTimeMillis();
			++containsQueries;
			boolean connected = mentions.areConnected(userPair[0], userPair[1]);
			long finish = System.currentTimeMillis();
			context.getCounter("Query", "Time").increment(finish - start);

			if (connected) {
				++numConnected;
				outvalue.set(userPair[0] + "->" + userPair[1] + "\t1");
				mos.write("complete", outkey, outvalue, "complete/part");
			} else {
				start = System.currentTimeMillis();
				Iterator<String> users = mentions.getConnections(userPair[0]);
				boolean hasConnection = false;
				while (users.hasNext()) {
					String user = users.next().toLowerCase();
					if (!value.toString().contains(user)) {
						hasConnection = true;
						++connections;
						outvalue.set(userPair[0] + "->" + user);
						mos.write("processing", outkey, outvalue,
								"processing/part");
					}
				}
				finish = System.currentTimeMillis();
				context.getCounter("Query", "Time").increment(finish - start);

				if (!hasConnection) {
					++deadend;
					outvalue.set(value);
					mos.write("complete", key, outvalue, "deadend/part");
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (mos != null) {
				mos.close();
			}
			context.getCounter("Query", "New Connections").increment(
					connections);
			context.getCounter("Query", "Contains").increment(containsQueries);
			context.getCounter("Query", "Dead Ends").increment(deadend);
			context.getCounter("Query", "Num Connected")
					.increment(numConnected);
		}
	}

	public static class TweetGraphBuilderTheSecond extends
			Mapper<Text, Text, Text, Text> {

		private Text outvalue = new Text();

		private UserMentions mentions = null;
		private MultipleOutputs<Text, Text> mos = null;
		private long connections = 0, containsQueries = 0, deadend = 0,
				numConnected = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				mentions = UserMentions.get(context.getConfiguration());
				mos = new MultipleOutputs<Text, Text>(context);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] userPair = key.toString().toLowerCase().split(" ");

			String[] userChain = value.toString().toLowerCase().split("->");
			int numUsers = userChain.length;

			// if the last user in the chain is connected to who we are seeking,
			// then output as complete
			long start = System.currentTimeMillis();
			++containsQueries;
			boolean connected = mentions.areConnected(userChain[numUsers - 1],
					userPair[1]);
			long finish = System.currentTimeMillis();
			context.getCounter("Query", "Time").increment(finish - start);
			if (connected) {
				++numConnected;
				outvalue.set(value + "->" + userPair[1] + "\t" + (numUsers - 1));
				mos.write("complete", key, outvalue, "complete/part");
			} else {
				start = System.currentTimeMillis();
				Iterator<String> users = mentions
						.getConnections(userChain[numUsers - 1]);
				boolean hasConnection = false;
				while (users.hasNext()) {
					String user = users.next().toLowerCase();
					if (!value.toString().contains(user)) {
						hasConnection = true;
						++connections;
						outvalue.set(value + "->" + user);
						mos.write("processing", key, outvalue,
								"processing/part");
					}
				}
				finish = System.currentTimeMillis();
				context.getCounter("Query", "Time").increment(finish - start);

				if (!hasConnection) {
					++deadend;
					outvalue.set(value);
					mos.write("complete", key, outvalue, "deadend/part");
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (mos != null) {
				mos.close();
			}
			context.getCounter("Query", "New Connections").increment(
					connections);
			context.getCounter("Query", "Contains").increment(containsQueries);
			context.getCounter("Query", "Dead Ends").increment(deadend);
			context.getCounter("Query", "Num Connected")
					.increment(numConnected);
		}
	}

	public static class NimbusMapSetWriterMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private String[] tokens, pair;
		private Text outkey = new Text(), outvalue = new Text();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			tokens = value.toString().split("\t", 10);

			if (!tokens[5].isEmpty()) {
				for (String entry : tokens[5].split(",")) {
					pair = entry.split(":");
					if (pair[0].equals(pair[1])) {
						// throw away pairs that are equal to one another
						return;
					}
					outkey.set(pair[0].toLowerCase());
					outvalue.set(pair[1].toLowerCase());
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class HBaseMapSetWriterMapper extends
			Mapper<LongWritable, Text, NullWritable, Put> {

		private String[] tokens, pair;
		private NullWritable outkey = NullWritable.get();
		private Put outvalue = null;
		private static final byte[] EMPTY_BYTES = new byte[0];

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			tokens = value.toString().split("\t", 10);

			if (!tokens[5].isEmpty()) {
				for (String entry : tokens[5].split(",")) {
					pair = entry.split(":");
					if (pair[0].equals(pair[1])) {
						// throw away pairs that are equal to one another
						return;
					}
					outvalue = new Put(Bytes.toBytes(pair[0].toLowerCase()));
					outvalue.add(HBaseUserMentions.COLUMN_FAMILY,
							Bytes.toBytes(pair[1].toLowerCase()), EMPTY_BYTES);

					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class UserMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private String[] tokens;
		private Text outkey = new Text();
		private LongWritable outvalue = new LongWritable(1);

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			tokens = value.toString().split("\t", 10);

			if (!tokens[3].isEmpty()) {
				outkey.set(tokens[3].toLowerCase());
				context.write(outkey, outvalue);

			}
		}
	}

	public static class NimbusMapSetReaderMapper extends
			Mapper<Text, Text, NullWritable, NullWritable> {

		private long start = 0, finish = 0;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			start = System.currentTimeMillis();
		}

		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// nothing yet
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			finish = System.currentTimeMillis();
			context.getCounter("NimbusMapSetReaderMapper", "Time (ms)")
					.increment(finish - start);
		}
	}

	public static class CombinedTextInputFormat extends
			CombineFileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException {
			return new CombineLineRecordReader();
		}

		public static class CombineLineRecordReader extends
				RecordReader<LongWritable, Text> {

			private LineRecordReader currRdr = new LineRecordReader();
			private int index = 0;
			private long total = 0;
			private CombineFileSplit split = null;
			private TaskAttemptContext context = null;

			@Override
			public void close() throws IOException {
				if (currRdr != null) {
					currRdr.close();
				}
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return currRdr.getCurrentKey();
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return currRdr.getCurrentValue();
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				if (total != 0 && currRdr != null) {
					return (currRdr.getProgress() + (float) ((index - 1)))
							/ (float) total;
				} else {
					return 1f;
				}
			}

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				this.split = (CombineFileSplit) split;
				total = split.getLength();
				this.context = context;

				if (total > 0) {
					advanceReader();
				}
			}

			private void advanceReader() throws IOException {
				if (currRdr != null) {
					currRdr.close();
				}

				try {
					if (index < split.getLength()) {
						currRdr = new LineRecordReader();
						currRdr.initialize(new FileSplit(split.getPath(index),
								split.getOffset(index), split.getLength(index),
								new String[0]), context);

						++index;
					} else {
						currRdr = null;
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					e.printStackTrace();
					currRdr = null;
				}
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {

				do {
					if (currRdr != null && currRdr.nextKeyValue()) {
						return true;
					} else if (currRdr == null) {
						break;
					} else {
						advanceReader();
					}
				} while (true);

				return false;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 5 && args.length != 6) {
			System.err
					.println("Usage: <raw_tweets> <users> <root_output_dir> <num users> <mentionsclassname> [skipingest]");
			return 1;
		}

		Path rawTweets = new Path(args[0]);
		Path users = new Path(args[1]);
		Path rootOutput = new Path(args[2]);
		int numUsers = Integer.parseInt(args[3]);
		String mentionsClass = args[4];

		Configuration conf = new Configuration();

		if (args.length != 6) {
			if (mentionsClass.contains("Nimbus")) {
				doNimbusIngest(conf, rawTweets);
				chattyNimbusUsers(users, numUsers);
			} else {
				doHBaseIngest(conf, rawTweets);
				chattyHBaseUsers(users, numUsers);
			}
		}
		

		chattyHBaseUsers(users, numUsers);

		int i = 0;
		Path input = null;
		Path outputDir = new Path(rootOutput + "/graph-" + i);
		if (startFirstGraphJob(conf, users, outputDir, mentionsClass) != 0L) {
			do {
				++i;
				input = new Path(outputDir + "/processing");
				outputDir = new Path(rootOutput + "/graph-" + i);
			} while (startOtherGraphJobs(conf, input, outputDir, mentionsClass,
					i) != 0L && i < 7);
		}

		return 0;
	}

	private void chattyNimbusUsers(Path outputfile, int numUsers)
			throws IOException, InterruptedException, ClassNotFoundException,
			CacheDoesNotExistException {
		MapSetClient client = new MapSetClient("twitter-mentions");

		TreeMap<Integer, Set<String>> usercounts = new TreeMap<Integer, Set<String>>();

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://nimbus-1:9000");

		String key = null;
		int count = 0;
		for (Entry<String, String> entry : client) {
			if (key == null) {
				key = entry.getKey();
			} else if (!key.equals(entry.getKey())) {
				Set<String> users = usercounts.get(count);
				if (users == null) {
					users = new HashSet<String>();
					usercounts.put(count, users);
				}

				users.add(key);

				key = entry.getKey();
				count = 0;
			}

			++count;
		}

		List<String> topUsers = new ArrayList<String>();
		FACES: for (Entry<Integer, Set<String>> entry : usercounts
				.descendingMap().entrySet()) {
			for (String user : entry.getValue()) {
				topUsers.add(user);
				if (topUsers.size() == numUsers) {
					break FACES;
				}
			}
		}

		PrintWriter wrtr = new PrintWriter(new OutputStreamWriter(FileSystem
				.get(conf).create(outputfile)));

		for (String userA : topUsers) {
			for (String userB : topUsers) {
				if (!userA.equals(userB)) {
					wrtr.println(userA + " " + userB);
				}
			}
		}

		wrtr.flush();
		wrtr.close();

		client.disconnect();

		System.out.println("Chatty users is complete");
	}

	private void chattyHBaseUsers(Path outputfile, int numUsers)
			throws IOException, InterruptedException, ClassNotFoundException,
			CacheDoesNotExistException {

		HTable table = new HTable(HBaseConfiguration.create(),
				"twitter-mentions");

		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(new Scan());

		TreeMap<Integer, Set<String>> usercounts = new TreeMap<Integer, Set<String>>();

		String key = null;
		int count = 0;
		for (Result r : scanner) {
			if (key == null) {
				key = Bytes.toString(r.getRow());
			} else if (!key.equals(Bytes.toString(r.getRow()))) {
				Set<String> users = usercounts.get(count);
				if (users == null) {
					users = new HashSet<String>();
					usercounts.put(count, users);
				}

				users.add(key);

				key = Bytes.toString(r.getRow());
				count = 0;
			}

			++count;
		}

		List<String> topUsers = new ArrayList<String>();
		FACES: for (Entry<Integer, Set<String>> entry : usercounts
				.descendingMap().entrySet()) {
			for (String user : entry.getValue()) {
				topUsers.add(user);
				if (topUsers.size() == numUsers) {
					break FACES;
				}
			}
		}

		PrintWriter wrtr = new PrintWriter(new OutputStreamWriter(FileSystem
				.get(new Configuration()).create(outputfile)));

		for (String userA : topUsers) {
			for (String userB : topUsers) {
				if (!userA.equals(userB)) {
					wrtr.println(userA + " " + userB);
				}
			}
		}

		wrtr.flush();
		wrtr.close();

		table.close();

		System.out.println("Chatty users is complete");
	}

	@SuppressWarnings("unchecked")
	private long startFirstGraphJob(Configuration conf, Path input,
			Path outputDir, String mentionsClass) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Twitter Graph - 1");
		job.setJarByClass(NimbusMapSetTest.class);

		job.setMapperClass(TweetGraphBuilderTheFirst.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);

		UserMentions.setMentionsClass(job.getConfiguration(),
				(Class<? extends UserMentions>) Class.forName(mentionsClass));
		UserMentions.setCacheName(job.getConfiguration(), "twitter-mentions");

		FileOutputFormat.setOutputPath(job, outputDir);
		MultipleOutputs.addNamedOutput(job, "complete", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "processing",
				SequenceFileOutputFormat.class, Text.class, Text.class);

		job.setMapSpeculativeExecution(false);

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();
		System.out.println("Took " + (finish - start) + " ms for job.");
		return job.getCounters().findCounter("Query", "New Connections")
				.getValue();
	}

	@SuppressWarnings("unchecked")
	private long startOtherGraphJobs(Configuration conf, Path input,
			Path outputDir, String mentionsClass, int num) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Twitter Graph - " + num);
		job.setJarByClass(NimbusMapSetTest.class);

		job.setMapperClass(TweetGraphBuilderTheSecond.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);

		UserMentions.setMentionsClass(job.getConfiguration(),
				(Class<? extends UserMentions>) Class.forName(mentionsClass));
		UserMentions.setCacheName(job.getConfiguration(), "twitter-mentions");

		FileOutputFormat.setOutputPath(job, outputDir);
		MultipleOutputs.addNamedOutput(job, "complete", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "processing",
				SequenceFileOutputFormat.class, Text.class, Text.class);

		job.setMapSpeculativeExecution(false);

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();
		System.out.println("Took " + (finish - start) + " ms for job.");

		return job.getCounters().findCounter("Query", "New Connections")
				.getValue();
	}

	private void doNimbusIngest(Configuration conf, Path input)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Mention Ingest-Nimbus");

		job.setJarByClass(NimbusMapSetTest.class);

		job.setMapperClass(NimbusMapSetWriterMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CombinedTextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);

		// job.setOutputFormatClass(TextOutputFormat.class);
		// TextOutputFormat.setOutputPath(job, new
		// Path("twitter-mentions"));

		job.setOutputFormatClass(NimbusMapSetOutputFormat.class);
		NimbusMapSetOutputFormat.setCacheName(job, "twitter-mentions");

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();

		System.out.println("Took " + (finish - start) + " ms for ingest.");
	}

	private void doHBaseIngest(Configuration conf, Path input)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Mention Ingest-HBase");

		job.setJarByClass(NimbusMapSetTest.class);

		job.setMapperClass(HBaseMapSetWriterMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Put.class);

		job.setInputFormatClass(CombinedTextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
				"twitter-mentions");

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long finish = System.currentTimeMillis();

		System.out.println("Took " + (finish - start) + " ms for ingest.");
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new NimbusMapSetTest(), args);
	}
}
