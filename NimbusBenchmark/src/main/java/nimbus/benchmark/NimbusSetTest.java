package nimbus.benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import nimbus.client.CacheletsUnavailableException;
import nimbus.client.StaticSetClient;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.CacheExistsException;
import nimbus.master.NimbusMaster;
import nimbus.utils.StaticSetIngestor;

public class NimbusSetTest {

	public static class Fif<T1, T2, T3, T4, T5> {
		T1 first;
		T2 second;
		T3 third;
		T4 fourth;
		T5 fifth;

		public Fif(T1 first, T2 second, T3 third, T4 fourth, T5 fifth) {
			this.first = first;
			this.second = second;
			this.third = third;
			this.fourth = fourth;
			this.fifth = fifth;
		}
	}

	private static BufferedWriter LOG = null;
	private static HashMap<String, Fif<Long, Long, Long, Long, Long>> ingestmap = new HashMap<String, Fif<Long, Long, Long, Long, Long>>();
	private static FileSystem fs;

	static {
		try {
			LOG = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File("nimbus-test-log"))));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException,
			CacheExistsException, CacheDoesNotExistException,
			InterruptedException, ClassNotFoundException {
		if (args.length != 1) {
			System.err
					.println("Usage: hadoop jar NimbusBenchmark.jar <input_dir>");
			System.exit(-1);
		}

		fs = FileSystem.get(new Configuration());

		System.out.println(fs.getConf().get("fs.default.name"));
		System.out.println(fs.getConf().get("mapred.job.tracker"));
		System.out.print("Connecting to master...");
		NimbusMaster master = NimbusMaster.getInstance();
		System.out.println(" Connected.");
		List<FileStatus> statuses = Arrays.asList(fs.listStatus(new Path(
				args[0])));

		Collections.sort(statuses);

		long ingesttime = 0, verifytime = 0, analytictime1 = 0, analytictime2 = 0, analytictime3 = 0;
		for (FileStatus status : statuses) {
			Path p = fs.makeQualified(status.getPath());
			System.out.println("Processing file " + p + "...");
			ingesttime = time(p);
			if (ingesttime == -1) {
				continue;
			}
			// verifytime = verify(fs.makeQualified(status.getPath()));
			analytictime1 = analytic(p, 0f);
			analytictime1 = analytic(p, .01f);
			analytictime2 = analytic(p, .05f);
			analytictime3 = analytic(p, .10f);
			ingestmap.put(p.toString(), new Fif<Long, Long, Long, Long, Long>(
					ingesttime, verifytime, analytictime1, analytictime2,
					analytictime3));

			Thread.sleep(15000);

			master.destroy(status.getPath().getName());
		}

		ArrayList<String> list = new ArrayList<String>();
		for (String e : ingestmap.keySet()) {
			list.add(e);
		}

		Collections.sort(list);

		for (String s : list) {
			Fif<Long, Long, Long, Long, Long> e = ingestmap.get(s);
			System.out.println("Ingested " + s + " in " + e.first + " ms.");
			LOG.write("Ingested " + s + " in " + e.first + " ms.\n");
			System.out.println("Verified " + s + " in " + e.second + " ms.");
			LOG.write("Verified " + s + " in " + e.second + " ms.\n");
			System.out.println("Analyzed 1% " + s + " in " + e.third + " ms.");
			LOG.write("Analyzed 1% " + s + " in " + e.third + " ms.\n");
			System.out.println("Analyzed 5% " + s + " in " + e.fourth + " ms.");
			LOG.write("Analyzed 5% " + s + " in " + e.fourth + " ms.\n");
			System.out.println("Analyzed 10% " + s + " in " + e.fifth + " ms.");
			LOG.write("Analyzed 10% " + s + " in " + e.fifth + " ms.\n");
		}

		LOG.flush();
		LOG.close();
		fs.close();
	}

	public static long time(Path p) throws CacheExistsException,
			CacheDoesNotExistException, IOException {
		long start = 0, finish = 0;
		String[] namechunks = p.getName().split("-");
		int numRecords = Integer.parseInt(namechunks[namechunks.length - 1]);
		LOG.write("Ingesting file " + p + "...");
		LOG.flush();
		start = System.currentTimeMillis();
		if (!StaticSetIngestor.ingest(true, p.getName(), p, numRecords, .05f)) {
			return -1;
		}
		finish = System.currentTimeMillis();
		LOG.write(" Done.  Took " + (finish - start) + "ms.\n");
		LOG.flush();
		return finish - start;
	}

	public static long verify(Path p) throws CacheDoesNotExistException,
			IOException {
		String[] namechunks = p.getName().split("-");
		float numRecords = Integer.parseInt(namechunks[namechunks.length - 1]);
		LOG.write("Verifying file " + p + "...");
		LOG.flush();
		long start = System.currentTimeMillis(), finish;
		StaticSetIngestor.verify(p.getName(), p);
		finish = System.currentTimeMillis();
		LOG.write(" Done.  Took " + (finish - start) + "ms.  Average "
				+ (float) (finish - start) / numRecords + " ms per record.\n");
		LOG.flush();
		return finish - start;
	}

	public static long analytic(Path p, float hitpercentage)
			throws IOException, InterruptedException, ClassNotFoundException {
		long start = 0, finish = 0;

		Path outdir = new Path("ingest-test-dir");

		if (fs.exists(outdir)) {
			fs.delete(outdir, true);
		}

		Job job = new Job(new Configuration(), "Nimbus Benchmark-"
				+ p.getName());
		job.setJarByClass(NimbusSetTest.class);
		System.out.println(job.getConfiguration().get("fs.default.name"));
		System.out.println(job.getConfiguration().get("mapred.job.tracker"));

		if (FileSystem.get(job.getConfiguration()).exists(outdir)) {
			FileSystem.get(job.getConfiguration()).delete(outdir, true);
		}

		TextInputFormat.addInputPath(job, p);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outdir);

		job.getConfiguration().set("nimbus.cache.name", p.getName());
		job.getConfiguration().set("mapred.map.tasks.speculative.execution",
				"false");
		job.getConfiguration().set("mapred.reduce.tasks.speculative.execution",
				"false");
		job.getConfiguration().set("hit.percentage",
				Float.toString(hitpercentage));
		job.getConfiguration().set("mapred.child.java.opts", "-Xmx512m");
		job.getConfiguration().set("mapred.task.timeout", "1800000");
		job.getConfiguration().set("MAPRED_MAP_TASK_ENV",
				"NIMBUS_HOME=/mnt/nimbus-git/Nimbus");

		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);

		LOG.write("Analyzing file " + p + " w/" + hitpercentage
				+ " hit percentage...");
		LOG.flush();

		start = System.currentTimeMillis();
		job.waitForCompletion(false);
		finish = System.currentTimeMillis();

		float hits = job.getCounters()
				.findCounter("NimbusBenchmark", "Num Hits").getValue();
		float accumtime = job.getCounters()
				.findCounter("NimbusBenchmark", "Contains Accum Time")
				.getValue();
		float dltime = job.getCounters()
				.findCounter("NimbusBenchmark", "BF Download Accum Time")
				.getValue();
		LOG.write(" Done.  Took "
				+ (finish - start)
				+ "ms.\nHits: "
				+ hits
				+ "  Accum Time: "
				+ accumtime
				+ "  Avg: "
				+ accumtime
				/ hits
				+ "\nDL Time: "
				+ dltime
				/ Float.parseFloat(job.getConfiguration().get(
						"mapred.map.tasks")) + "\n");
		LOG.flush();
		return finish - start;
	}

	private static class MyMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private StaticSetClient set = null;
		private Random rndm = new Random();
		private float hitPercentage = 0.0f;
		private long start, finish;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				long start = System.currentTimeMillis();
				set = new StaticSetClient(context.getConfiguration().get(
						"nimbus.cache.name"));
				long end = System.currentTimeMillis();
				context.getCounter("NimbusBenchmark", "BF Download Accum Time")
						.increment(end - start);
				hitPercentage = Float.parseFloat(context.getConfiguration()
						.get("hit.percentage"));
			} catch (CacheDoesNotExistException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rndm.nextFloat() < hitPercentage) {
				do {
					try {
						start = System.currentTimeMillis();
						if (!set.contains(value.toString())) {
							context.getCounter("NimbusBenchmark",
									"Records that DNE").increment(1);
						}
						finish = System.currentTimeMillis();
						break;
					} catch (CacheletsUnavailableException e) {
						Thread.sleep(1000);
						context.progress();
						context.getCounter("NimbusBenchmark",
								"Exceptions Caught").increment(1);
					}
				} while (true);

				context.getCounter("NimbusBenchmark", "Contains Accum Time")
						.increment(finish - start);
				context.getCounter("NimbusBenchmark", "Num Hits").increment(1);
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (set != null) {
				set.disconnect();
			}
		}
	}
}
