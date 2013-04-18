package nimbus.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class NimbusBenchmark extends Configured implements Tool {

	public static String makeNimbusSafe(String str) {
		String tmp = str.replaceAll("\\s", "");
		while (tmp.length() < 100) {
			tmp += 'A';
		}
		return tmp;
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

		FileStatus[] files = FileSystem.get(getConf()).listStatus(input);

		for (FileStatus status : files) {
			LOG.info("Processing file " + status.getPath());
			if (type.equalsIgnoreCase("nimbus")) {
				NimbusSetBenchmarker benchmarker = new NimbusSetBenchmarker();
				benchmarker.setConf(getConf());
				benchmarker.run(status.getPath(), cacheName + "-" + status.getPath().getName());
			} else if (type.equalsIgnoreCase("hbase")) {
				HBaseBenchmarker benchmarker = new HBaseBenchmarker();
				benchmarker.setConf(getConf());
				benchmarker.run(status.getPath(), cacheName + "-" + status.getPath().getName());
			} else {
				LOG.error("Unknown type " + type);
				return 1;
			}
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new NimbusBenchmark(), args);
	}
}
