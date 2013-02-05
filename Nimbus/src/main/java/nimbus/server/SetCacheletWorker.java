package nimbus.server;

import java.io.IOException;

import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class SetCacheletWorker extends ICacheletWorker {

	private static final Logger LOG = Logger
			.getLogger(SetCacheletWorker.class);

	/**
	 * The DIS_READ command will ingest a file into HDFS and keep members based
	 * on the given Cachelet ID.
	 */
	public static final int DIS_READ = 3;

	/**
	 * The CONTAINS command will determine if a given element is a member of
	 * this Cachelet's set.
	 */
	public static final int CONTAINS = 4;

	/**
	 * The ISEMPTY command will return a value based on if this Cachelet is
	 * empty or not.
	 */
	public static final int ISEMPTY = 5;

	private SetCacheletServer server = null;
	private static final String HELP_MESSAGE = "Invalid input.";

	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public SetCacheletWorker(SetCacheletServer server) {
		this.server = server;
	}

	@Override
	public void processMessage(String theInput) throws IOException {
		String[] tokens = theInput.split("\\s");
		int cmd = Integer.parseInt(tokens[0]);

		if (tokens.length == 5) {
			processFiveArgs(cmd, tokens);
		} else if (tokens.length == 2) {
			processTwoArgs(cmd, tokens);
		} else if (tokens.length == 1) {
			processOneArg(cmd, tokens);
		} else {
			printHelpMessage(tokens);
		}

		out.flush();
	}

	private void processOneArg(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case ISEMPTY:
			out.write(server.isEmpty() + "\n");
			break;
		default:
			printHelpMessage(tokens);
		}
	}

	private void processTwoArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case CONTAINS:
			out.write(server.contains(tokens[1]) + "\n");
			break;
		default:
			printHelpMessage(tokens);
			break;
		}
	}

	private void processFiveArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case DIS_READ:
			out.write(server.distributedLoadFromHDFS(new Path(tokens[3]),
					Integer.parseInt(tokens[1]), Float.parseFloat(tokens[2]),
					Integer.parseInt(tokens[4]))
					+ "\n");
			break;
		default:
			printHelpMessage(tokens);
			break;
		}
	}

	private void printHelpMessage(String[] tokens) throws IOException {
		String errmsg = "";

		for (int i = 0; i < tokens.length; ++i) {
			errmsg += tokens[i] + " ";
		}

		LOG.error("Received unknown message: " + errmsg);
		out.write(HELP_MESSAGE + "\n");
	}
}
