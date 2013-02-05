package nimbus.server;

import java.io.IOException;

import nimbus.main.NimbusConf;

import org.apache.log4j.Logger;

import nimbus.master.NimbusMaster;

public class MasterCacheletWorker extends ICacheletWorker {

	private final Logger LOG = Logger.getLogger(MasterCacheletWorker.class);

	public static final String DESTROYCMD = "k";
	public static final String CREATECMD = "c";

	private static final String HELP_MESSAGE = "Invalid input";

	public MasterCacheletWorker() {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}
	
	@Override
	public void processMessage(String theInput) throws IOException {

		LOG.debug("Received command: " + theInput);

		String[] tokens = theInput.split("\\s");

		if (tokens.length == 3) {
			processThreeArgs(tokens);
		} else if (tokens.length == 2) {
			processTwoArgs(tokens);
		} else {
			printHelpMessage(tokens);
		}

		out.flush();
	}

	private void processThreeArgs(String[] tokens) throws IOException {
		if (tokens[0].equals(CREATECMD)) {
			try {
				NimbusMaster.getInstance().create(tokens[1], CacheType.valueOf(tokens[2]
						.toUpperCase()));
				out.write("true\n");
			} catch (IOException e) {
				LOG.error(e.getMessage());
				out.write("false\n");
			}
		} else {
			printHelpMessage(tokens);
		}
	}

	private void processTwoArgs(String[] tokens) throws IOException {
		if (tokens[0].equals(DESTROYCMD)) {
			out.write(NimbusMaster.getInstance().destroy(tokens[1]) + "\n");
		} else {
			printHelpMessage(tokens);
		}
	}

	private void printHelpMessage(String[] tokens) throws IOException {
		String errmsg = "";
		for (int i = 0; i < tokens.length; ++i) {
			errmsg += tokens[i] + " ";
		}

		LOG.error("Received unknown message:" + errmsg);
		out.write(HELP_MESSAGE + "\n");
	}
}