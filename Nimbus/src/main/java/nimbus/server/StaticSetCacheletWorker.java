package nimbus.server;

import java.io.IOException;

import org.apache.log4j.Logger;

public class StaticSetCacheletWorker extends ICacheletWorker {

	private static final Logger LOG = Logger.getLogger(StaticSetCacheletWorker.class);

	/**
	 * The CONTAINS command will determine if a given element is a member of
	 * this Cachelet's set.
	 */
	public static final int CONTAINS = 1;

	/**
	 * The ISEMPTY command will return a value based on if this Cachelet is
	 * empty or not.
	 */
	public static final int ISEMPTY = 2;

	/**
	 * The GET command will stream all values back to the user
	 */
	public static final int GET = 3;

	private StaticSetCacheletServer server = null;
	private static final String HELP_MESSAGE = "Invalid input.";

	public StaticSetCacheletWorker(StaticSetCacheletServer server) {
		this.server = server;
	}

	@Override
	public void processMessage(String theInput) throws IOException {
		String[] tokens = theInput.split("\\s");
		int cmd = Integer.parseInt(tokens[0]);

		if (tokens.length == 2) {
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
		case GET:
			out.write(server.size() + "\n");
			for (String key : server) {
				out.write(key + "\n");
			}
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

	private void printHelpMessage(String[] tokens) throws IOException {
		String errmsg = "";

		for (int i = 0; i < tokens.length; ++i) {
			errmsg += tokens[i] + " ";
		}

		LOG.error("Received unknown message: " + errmsg);
		out.write(HELP_MESSAGE + "\n");
	}
}
