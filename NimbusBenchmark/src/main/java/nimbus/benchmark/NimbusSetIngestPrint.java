package nimbus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import nimbus.client.StaticSetClient;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.CacheExistsException;
import nimbus.master.NimbusMaster;
import nimbus.utils.StaticSetIngestor;

public class NimbusSetIngestPrint {

	private static FileSystem fs;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException,
			CacheExistsException, CacheDoesNotExistException,
			InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err
					.println("Usage: hadoop jar NimbusBenchmark.jar <input_dir> <output_dir>");
			System.exit(-1);
		}

		fs = FileSystem.get(new Configuration());

		System.out.print("Connecting to master...");
		NimbusMaster master = NimbusMaster.getInstance();

		List<FileStatus> statuses = Arrays.asList(fs.listStatus(new Path(
				args[0])));

		Collections.sort(statuses);

		long ingesttime = 0;
		for (FileStatus status : statuses) {
			Path p = fs.makeQualified(status.getPath());
			System.out.println("Processing file " + p + "...");
			ingesttime = time(p);

			if (ingesttime == -1) {
				continue;
			}

			for (int i = 0; i < 5; ++i) {
				long startTime = System.currentTimeMillis();

				StaticSetClient client = new StaticSetClient(p.getName());
				int numRecords = 0;

				for (@SuppressWarnings("unused")
				String s : client) {
					++numRecords;
				}

				client.disconnect();

				long endTime = System.currentTimeMillis();

				System.out.println("CACHE Read " + numRecords + " in "
						+ (endTime - startTime) + " ms.");

				startTime = System.currentTimeMillis();

				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(p)));
				numRecords = 0;
				while (rdr.readLine() != null) {
					++numRecords;
				}

				endTime = System.currentTimeMillis();
				System.out.println("FILE Read " + numRecords + " in "
						+ (endTime - startTime) + " ms.");
			}

			master.destroy(status.getPath().getName());
		}
	}

	public static long time(Path p) throws CacheExistsException,
			CacheDoesNotExistException, IOException {
		long start = 0, finish = 0;
		String[] namechunks = p.getName().split("-");

		int numRecords = Integer.parseInt(namechunks[namechunks.length - 1]);
		start = System.currentTimeMillis();

		if (!StaticSetIngestor.ingest(true, p.getName(), p, numRecords, .05f)) {
			return -1;
		}

		finish = System.currentTimeMillis();

		return finish - start;
	}
}
