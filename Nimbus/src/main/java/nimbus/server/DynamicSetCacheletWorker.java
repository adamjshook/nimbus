package nimbus.server;

import java.io.IOException;

import org.apache.log4j.Logger;

public class DynamicSetCacheletWorker extends ICacheletWorker {

	private static final Logger LOG = Logger
			.getLogger(DynamicSetCacheletWorker.class);

	public static final int ADD = 1;
	public static final int ADD_ALL = 2;
	public static final int CLEAR = 3;

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
	 * The GET command will stream all values back to the user
	 */
	public static final int GET = 6;

	public static final int REMOVE = 7;
	public static final int RETAIN_ALL = 8;
	public static final int SIZE = 9;

	private DynamicSetCacheletServer server = null;
	private static final String HELP_MESSAGE = "Invalid input.";

	public DynamicSetCacheletWorker(DynamicSetCacheletServer server) {
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
		case SIZE:
			out.write(server.size() + "\n");
			break;
		case CLEAR:
			server.clear();
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
		case ADD:
			out.write(server.add(tokens[1]) + "\n");
			break;
		case ADD_ALL:
			int numEntries = Integer.parseInt(tokens[1]);
			for (int i = 0; i < numEntries; ++i) {
				server.add(readLine());
			}
			break;
		case REMOVE:
			out.write(server.remove(tokens[1]) + "\n");
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
