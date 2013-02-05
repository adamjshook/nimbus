package nimbus.server;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketException;

import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public abstract class ICacheletWorker implements Runnable {

	private final Logger LOG = Logger.getLogger(ICacheletWorker.class);
	protected OutputStreamWriter out = null;
	protected InputStreamReader in = null;

	/**
	 * Creates an {@link IProtocol} object based on the given {@link CacheType}
	 * to respond to requests.
	 * 
	 * @param type
	 *            The CacheType.
	 * @param socket
	 *            Gets the I/O streams from the socket.
	 * @throws IOException
	 *             If an error occurs when retrieving the streams, or this
	 *             Cachelet does not support the given type.
	 */
	public void initialize(String cacheName, String cacheletName,
			CacheType type, Socket socket) throws IOException {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
		out = new OutputStreamWriter(socket.getOutputStream());
		in = new InputStreamReader(socket.getInputStream());
	}
	
	/**
	 * Processes requests from the client until the end of the stream is read or
	 * the "kill" command is received.
	 */
	@Override
	public void run() {
		try {
			String inputLine = null;
			while ((inputLine = readLine()) != null) {
				try {
					if (inputLine.equals("kill")) {
						LOG
								.info("Kill command received. Deleting Bloom filter from HDFS and exiting...");

						FileSystem.get(NimbusConf.getConf()).delete(
								new Path(Nimbus.CACHELET_ZNODE), false);

						System.exit(0);
					}

					processMessage(inputLine);
				} catch (Exception e) {
					LOG.error("Caught exception while processing input");
					e.printStackTrace();
					out
							.write("Caught exception: " + e.getMessage()
									+ "\nEOS\n");
					out.flush();
				}
			}

			out.close();
			in.close();
		} catch (SocketException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
	}
	
	protected abstract void processMessage(String msg) throws IOException;	
	
	/**
	 * Reads a line of input from the input stream.<br>
	 * <br>
	 * This method reads a single character at a time and uses the StringBuilder
	 * utility. Note that this method will block until a newline character is
	 * read or if the end of the stream has been reached.
	 * 
	 * @return A line of text from the input stream or null if there is no
	 *         message.
	 * @throws IOException
	 */
	protected String readLine() throws IOException {
		StringBuilder builder = new StringBuilder();
		int c;
		while ((c = in.read()) != '\n' && c != -1) {
			builder.append((char) c);
		}

		return builder.toString().length() == 0 ? null : builder.toString();
	}
}
