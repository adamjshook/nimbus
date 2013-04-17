package nimbus.server;

import java.io.IOException;

import nimbus.utils.BytesUtil;
import nimbus.utils.NimbusInputStream;

import org.apache.log4j.Logger;

public class DynamicSetCacheletWorker extends ICacheletWorker {

	@SuppressWarnings("unused")
	private static final Logger LOG = Logger
			.getLogger(DynamicSetCacheletWorker.class);

	public static final int ADD_CMD = 1;
	public static final int ADD_ALL_CMD = 2;
	public static final int CLEAR_CMD = 3;

	/**
	 * The CONTAINS command will determine if a given element is a member of
	 * this Cachelet's set.
	 */
	public static final int CONTAINS_CMD = 4;

	/**
	 * The ISEMPTY command will return a value based on if this Cachelet is
	 * empty or not.
	 */
	public static final int ISEMPTY_CMD = 5;

	/**
	 * The GET command will stream all values back to the user
	 */
	public static final int GET_CMD = 6;

	public static final int REMOVE_CMD = 7;
	public static final int RETAIN_ALL_CMD = 8;
	public static final int SIZE_CMD = 9;
	public static final int ACK_CMD = 10;

	private DynamicSetCacheletServer server = null;

	public DynamicSetCacheletWorker(DynamicSetCacheletServer server) {
		this.server = server;
	}

	@Override
	public void processMessage(int cmd, long numArgs, NimbusInputStream rdr)
			throws IOException {

		switch (cmd) {
		case ISEMPTY_CMD:
			out.write(ACK_CMD, String.valueOf(server.isEmpty()));
			break;
		case GET_CMD:
			out.prepStreamingWrite(ACK_CMD, server.size());

			for (String key : server) {
				out.streamingWrite(key);
			}

			out.endStreamingWrite();

			break;
		case SIZE_CMD:
			out.write(ACK_CMD, String.valueOf(server.size()));
			break;
		case CLEAR_CMD:
			server.clear();
			break;
		case CONTAINS_CMD:
			out.write(ACK_CMD,
					String.valueOf(server.contains(BytesUtil.toString(rdr.readArg()))));
			break;
		case ADD_CMD:
			out.write(ACK_CMD,
					String.valueOf(server.add(BytesUtil.toString(rdr.readArg()))));
			break;
		case ADD_ALL_CMD:
			for (int i = 0; i < numArgs; ++i) {
				server.add(BytesUtil.toString(rdr.readArg()));
			}
			break;
		case REMOVE_CMD:
			out.write(ACK_CMD,
					String.valueOf(server.remove(BytesUtil.toString(rdr.readArg()))));
			break;
		default:
			printHelpMessage(cmd, numArgs, rdr);
		}
	}
}
