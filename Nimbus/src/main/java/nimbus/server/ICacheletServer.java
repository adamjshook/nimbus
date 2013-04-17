package nimbus.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import nimbus.nativestructs.CSet;

import org.apache.log4j.Logger;

/**
 * A Cachelet opens up a server on the given port and creates an appropriate
 * {@link IProtocol} based on the given {@link CacheType}. <br>
 * <br>
 * A Cachelet is responsible for creating threads to handle each client
 * connection, as well as holding the object that represents (such as a
 * {@link CSet}). <br>
 * <br>
 * This class contains a private class to actually respond to messages sent by
 * clients.
 */
public abstract class ICacheletServer implements Runnable {

	private static final Logger LOG = Logger.getLogger(ICacheletServer.class);

	protected List<ICacheletWorker> workers = new ArrayList<ICacheletWorker>();

	protected int port = 0;
	protected CacheType type = null;
	protected String cacheName = null;
	protected String cacheletName = null;

	private ServerSocket serverSocket;

	/**
	 * Initializes a new instance of the {@link ICacheletServer} class. <br>
	 * <br>
	 * Does not actually create the server until {@link ICacheletServer#run()}
	 * is called.
	 * 
	 * @param port
	 *            The port to create the server on.
	 * @param type
	 *            The type of Cache to create.
	 */
	public ICacheletServer(String cacheName, String cacheletName, int port,
			CacheType type) {
		this.port = port;
		this.type = type;
		this.cacheName = cacheName;
		this.cacheletName = cacheletName;
	}

	protected abstract ICacheletWorker getNewWorker();

	/**
	 * Opens up the server and creates a thread for each connection to it.
	 */
	@Override
	public void run() {
		openServer();
		acceptConnections();
	}

	protected void openServer() {
		LOG.info("Opening up server on port " + port);
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			LOG.error("Could not listen on port " + port);
			System.exit(1);
		}
	}

	protected void acceptConnections() {
		while (true) {
			ICacheletWorker w = null;
			try {
				w = getNewWorker();
				w.initialize(this, cacheName, cacheletName, type,
						serverSocket.accept());

				Thread t = new Thread(w);
				t.start();
				LOG.info("Started a new worker");
			} catch (IOException e) {
				LOG.error("Accept failed: " + e.getMessage());
				System.exit(1);
			}
		}
	}

	public void shutdown() {
		System.exit(0);
	}
}