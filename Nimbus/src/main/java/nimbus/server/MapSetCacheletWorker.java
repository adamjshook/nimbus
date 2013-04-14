package nimbus.server;

import java.io.IOException;
import java.util.Map.Entry;

import nimbus.nativestructs.CSet;

import org.apache.log4j.Logger;

public class MapSetCacheletWorker extends ICacheletWorker {

	private static final Logger LOG = Logger
			.getLogger(MapSetCacheletWorker.class);

	public static final int CONTAINS_KEY = 1;
	public static final int CONTAINS_KEY_VALUE = 2;
	public static final int ISEMPTY = 3;
	public static final int ADD = 4;
	public static final int REMOVE_KEY = 5;
	public static final int REMOVE_KEY_VALUE = 6;
	public static final int CLEAR = 7;
	public static final int SIZE = 8;
	public static final int GET = 9;
	public static final int GET_ALL = 10;

	private MapSetCacheletServer server = null;
	private static final String HELP_MESSAGE = "Invalid input.";

	public MapSetCacheletWorker(MapSetCacheletServer server) {
		this.server = server;
	}

	@Override
	public void processMessage(String theInput) throws IOException {
		String[] tokens = theInput.split("\\s");
		int cmd = Integer.parseInt(tokens[0]);

		if (tokens.length == 3) {
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
		case CLEAR:
			server.clear();
			break;
		case SIZE:
			out.write(server.size() + "\n");
			break;
		case GET_ALL:
			// synchronize on server instance (this), have a thread to push out
			// all the values
			out.write(server.size() + "\n");
			String key;
			for (Entry<String, CSet> entry : server) {
				key = entry.getKey();
				for (String value : entry.getValue()) {
					out.write(key + "\t" + value + "\n");
				}
			}
			break;
		default:
			printHelpMessage(tokens);
		}
	}

	private void processTwoArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case CONTAINS_KEY:
			out.write(server.contains(tokens[1]) + "\n");
			break;
		case REMOVE_KEY:
			server.remove(tokens[1]);
			break;
		case GET:
			CSet set = server.get(tokens[1]);
			if (set != null) {
				out.write(set.size() + "\n");
				for (String value : set) {
					out.write(value + "\n");
				}
			} else {
				out.write("0\n");
			}

			break;
		default:
			printHelpMessage(tokens);
			break;
		}
	}

	private void processThreeArgs(int cmd, String[] tokens) throws IOException {
		switch (cmd) {
		case REMOVE_KEY_VALUE:
			server.remove(tokens[1], tokens[2]);
			break;
		case CONTAINS_KEY_VALUE:
			out.write(server.contains(tokens[1], tokens[2]) + "\n");
			break;
		case ADD:
			server.add(tokens[1], tokens[2]);
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
