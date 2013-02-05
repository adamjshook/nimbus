package nimbus.server;

import java.io.IOException;
import java.util.Iterator;

import nimbus.main.NimbusConf;
import nimbus.utils.Triple;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TripleSetCacheletWorker extends ICacheletWorker {

	private static final Logger LOG = Logger
			.getLogger(TripleSetCacheletWorker.class);

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

	/**
	 * The ADD command returns a Boolean value is the Cachelet successfully
	 * added the given Triple
	 */
	public static final int ADD = 6;

	public static final int GET_ALL = 7;
	public static final int GET_WITH_ONE = 8;
	public static final int GET_WITH_TWO = 9;

	public static final String EOS = "EOS\n";

	private TripleSetCacheletServer server = null;
	private static final String HELP_MESSAGE = "Invalid input.";

	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public TripleSetCacheletWorker(TripleSetCacheletServer server) {
		this.server = server;
	}

	@Override
	public void processMessage(String theInput) throws IOException {
		String[] tokens = theInput.split("\\s");
		int cmd = Integer.parseInt(tokens[0]);

		if (tokens.length == 4) {
			processFourArgs(cmd, tokens);
		} else if (tokens.length == 3) {
			processThreeArgs(cmd, tokens);
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
		case GET_ALL:
			Iterator<Triple> iter = server.iterator(tokens[1]);
			while (iter.hasNext()) {
				out.write(iter.next() + "\n");
			}
			out.write(EOS);

			break;
		default:
			printHelpMessage(tokens);
		}
	}

	private void processTwoArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case GET_WITH_ONE:
			Iterator<Triple> iter = server.iterator(tokens[1]);
			while (iter.hasNext()) {
				out.write(iter.next() + "\n");
			}
			out.write(EOS);

			break;
		case DIS_READ:
			out.write(server.distributedLoadFromHDFS(new Path(tokens[1]),
					Integer.parseInt(tokens[2])) + "\n");
			break;
		default:
			printHelpMessage(tokens);
			break;
		}
	}

	private void processThreeArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case GET_WITH_TWO:
			Iterator<Triple> iter = server.iterator(tokens[1], tokens[2]);
			while (iter.hasNext()) {
				out.write(iter.next() + "\n");
			}

			break;
		default:
			printHelpMessage(tokens);
			break;
		}
	}

	private void processFourArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case CONTAINS:
			out.write(server.contains(tokens[1], tokens[2], tokens[3]) + "\n");
			break;
		case ADD:
			out.write(server.add(tokens[1], tokens[2], tokens[3]) + "\n");
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
