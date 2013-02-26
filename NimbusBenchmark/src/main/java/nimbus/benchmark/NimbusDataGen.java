package nimbus.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import nimbus.main.NimbusConf;

public class NimbusDataGen {

	public static final Logger LOG = Logger.getLogger(NimbusDataGen.class);

	private static FileSystem fs;

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err
					.println("Usage: hadoop jar nimbus.jar nimbus.datagen.NimbusDataGen <output_dir> <csv_numbytes> <csv_numchars>");
			System.exit(-1);
		}

		Path dir = new Path(args[0]);
		LOG.info("Getting file system: "
				+ NimbusConf.getConf().get("fs.default.name"));
		fs = FileSystem.get(NimbusConf.getConf());

		if (fs.exists(dir)) {
			fs.delete(dir, true);
		}

		if (!fs.mkdirs(dir)) {
			System.err.println("Failed to make directory " + dir);
			System.exit(-1);
		}

		String[] numBytes = args[1].split(",");
		String[] numChars = args[2].split(",");

		// verify longs
		for (String bytes : numBytes) {
			Long.parseLong(bytes);
		}

		// verify ints
		for (String chars : numChars) {
			Integer.parseInt(chars);
		}

		for (String bytes : numBytes) {
			for (String chars : numChars) {
				Path p = new Path(dir + "/" + "data-" + bytes + "B-" + chars
						+ "c");
				System.out.print("Generating " + fs.makeQualified(p) + "...");
				long records = genFile(p, Long.parseLong(bytes),
						Integer.parseInt(chars));
				System.out.println(" Done.  Wrote " + records);

				fs.rename(p, new Path(p + "-" + records));
			}
		}
		System.out.println("All files created.");
		System.exit(0);
	}

	private static long genFile(Path p, long numbytes, int numchars)
			throws IOException {
		BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(
				fs.create(p)));

		long byteswritten = 0, records = 0;
		String tmp = null;
		while (byteswritten < numbytes) {
			tmp = generateRandomString(numchars);
			if (byteswritten + tmp.getBytes().length + 1 > numbytes) {
				break;
			} else {
				++records;
				wrtr.write(tmp + "\n");
				byteswritten += tmp.getBytes().length + 1;
			}
		}

		wrtr.close();
		return records;
	}

	/**
	 * Generates a random string. Looks a lot like a UUID because it is a
	 * concatenation of random UUID.
	 * 
	 * @returne	 A random string.
	 */
	private static String generateRandomString(int numchars) {
		String tmp = "";

		do {
			tmp += UUID.randomUUID().toString();
		} while (tmp.length() <= numchars);

		return tmp.substring(0, numchars);
	}
}
