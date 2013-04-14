package nimbus.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.Socket;

import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import nimbus.utils.DataZNodeWatcher;

/**
 * This is the base class for all clients in Nimbus. It handles connecting to a
 * Cachelet on a given host and port.<br>
 * <br>
 * It has helpful methods for broadcasting.retrieving messages to/from the
 * Cachelet.
 */
public class BaseNimbusClient implements Runnable {

	protected static FileSystem fs = null;
	private static final Logger LOG = Logger.getLogger(BaseNimbusClient.class);
	protected DataZNodeWatcher watcher = new DataZNodeWatcher();

	static {
		try {
			fs = FileSystem.get(NimbusConf.getConf());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected OutputStreamWriter out = null;
	protected InputStreamReader in = null;

	protected String host = null;
	protected int port = 0;
	protected Socket echoSocket = null;
	protected String cacheName;
	protected boolean connected = false;

	/**
	 * Sets the client to bind to the given host and port. Doesn't actually
	 * connect until {@link BaseNimbusClient#connect()}is called.
	 * 
	 * @param host
	 *            The machine address.
	 * @param port
	 *            The port to connect to.
	 */
	public BaseNimbusClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	/**
	 * Closes the stream just in case the user didn't.
	 */
	@Override
	protected void finalize() throws Throwable {
		disconnect();
		super.finalize();
	}

	/**
	 * Threaded functionality of the client. Used by Nimbus to handle
	 * communications between Cachelets. Users of Nimbus should call
	 * {@link BaseNimbusClient#connect()}.
	 */
	@Override
	public void run() {
		try {
			connect();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	/**
	 * Connects this client to the host and port given during initialization.
	 * Sets the out and in protected member variables to the streams on the
	 * socket.
	 */
	public void connect() throws IOException {
		if (connected) {
			return;
		}

		long sleep = 1000;
		int failattempts = 10;
		while (!connected) {
			try {
				echoSocket = new Socket(host, port);
				connected = true;
				LOG.info("Connected to " + host + " on port " + port);
			} catch (ConnectException e) {
				--failattempts;
				if (failattempts != 0) {
					LOG.error("Failed to connect to " + host + " on port "
							+ port + ".  Sleeping for " + sleep
							+ " ms and retrying " + failattempts
							+ " more times...");
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					throw new IOException(
							"Failed to connect to host too many times.");
				}
			}
		}

		out = new OutputStreamWriter(echoSocket.getOutputStream());
		in = new InputStreamReader(echoSocket.getInputStream());

		LOG.debug("Connected to " + host + " on port " + port);
	}

	/**
	 * Reads from the input stream until EOF is reached.
	 */
	public void dumpResponses() {
		try {
			if (in != null && in.ready()) {
				while (in.read() != -1) {
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Closes the socket and I/O streams.
	 */
	public void disconnect() {
		try {
			connected = false;
			if (out != null) {
				out.close();
				out = null;
			}
			if (in != null) {
				in.close();
				in = null;
			}
			if (echoSocket != null) {
				echoSocket.close();
				echoSocket = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Writes the given String and a newline character to the output stream and
	 * flushes the stream.
	 * 
	 * @param msg
	 *            The string to broadcast
	 * @throws IOException
	 *             If an error occurs when sending the message.
	 */
	public void writeLine(String msg) throws IOException {
		if (connected) {
			out.write(msg + "\n");
			out.flush();
		} else {
			throw new CacheletNotConnectedException();
		}
	}

	/**
	 * Gets a value indicating if this client is currently connected.
	 * 
	 * @return Whether or not this client is currently connected.
	 */
	public boolean isConnected() {
		return connected;
	}

	/**
	 * Helper function for clients to retrieve a line of text from the input
	 * stream.
	 * 
	 * @param input
	 * @return The line of text from the input string.
	 * @throws IOException
	 *             If an error occurs when reading the message.
	 * @throws EOFException
	 *             If the end of the stream is reached.
	 */
	protected String readLine() throws IOException {
		StringBuilder builder = new StringBuilder();
		int c;
		while ((c = in.read()) != '\n') {
			if (c == -1) {
				throw new EOFException();
			}
			builder.append((char) c);
		}

		return builder.toString();
	}
}