package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedBytes;

public class NimbusBenchmark extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(NimbusBenchmark.class);

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3 && args.length != 4) {
			System.err
					.println("Usage: <input> <cachename> [nimbus|hbase <splitsfile>]");
			return 1;
		}

		Path input = new Path(args[0]);
		String cacheName = args[1];
		String type = args[2];

		FileStatus[] files = FileSystem.get(getConf()).listStatus(input);

		for (FileStatus status : files) {
			LOG.info("Processing file " + status.getPath());
			if (type.equalsIgnoreCase("nimbus")) {
				NimbusSetBenchmarker benchmarker = new NimbusSetBenchmarker();
				benchmarker.setConf(getConf());
				benchmarker.run(status.getPath(), cacheName + "-"
						+ status.getPath().getName());
			} else if (type.equalsIgnoreCase("hbase")) {

				BufferedReader rdr = new BufferedReader(new FileReader(args[3]));
				TreeSet<byte[]> strSplits = new TreeSet<byte[]>(
						UnsignedBytes.lexicographicalComparator());
				String s;
				while ((s = rdr.readLine()) != null) {
					strSplits.add(s.getBytes());
				}

				byte[][] bytes = new byte[strSplits.size()][];

				int i = 0;
				for (byte[] b : strSplits) {
					bytes[i++] = b;
				}

				HBaseBenchmarker benchmarker = new HBaseBenchmarker();
				benchmarker.setConf(HBaseConfiguration.create(getConf()));
				benchmarker.run(status.getPath(), cacheName + "-"
						+ status.getPath().getName(), bytes);
			} else {
				LOG.error("Unknown type " + type);
				return 1;
			}

			LOG.info("Sleeping for 5 seconds before next file...");
			Thread.sleep(5000);
		}

		return 0;

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new NimbusBenchmark(), args);
	}
}

