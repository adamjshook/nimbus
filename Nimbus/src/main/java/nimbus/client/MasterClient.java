package nimbus.client;

import java.io.IOException;
import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.log4j.Logger;

import nimbus.master.CacheExistsException;
import nimbus.master.NimbusMaster;
import nimbus.master.FailedToCreateCacheException;
import nimbus.server.CacheType;
import nimbus.server.MasterCacheletWorker;

/**
 * The MasterClient should be utilized whenever applications need to create or
 * destroy Caches. There are other useful functions related to the Master
 * service that is in {@link NimbusMaster}. <br>
 * <br>
 * This class may be deprecated in the future and the functionality moved to
 * {@link NimbusMaster}.
 */
public class MasterClient extends BaseNimbusClient {

	private static final Logger LOG = Logger.getLogger(MasterClient.class);
	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public MasterClient() throws IOException {
		super(Nimbus.getRandomNimbusHost(), Integer.parseInt(NimbusConf
				.getConf().getNimbusMasterPort()));
		connect();
	}

	/**
	 * Simply a helper function that wraps {@link NimbusMaster#exists(String)}.<br>
	 * Determines if the Cache exists or not.
	 * 
	 * @param cacheName
	 *            The Cache to check.
	 * @return If the Cache exists.
	 */
	public boolean exists(String cacheName) {
		return NimbusMaster.getInstance().exists(cacheName);
	}

	/**
	 * Sends a request to the Master service to create a Cache.
	 * 
	 * @param cacheName
	 *            A unique Cache name.
	 * @param type
	 *            The type of Cache to create.
	 * @return If the operation was successful or not.
	 * @throws CacheExistsException
	 *             If the Cache already exists.
	 * @throws IOException
	 *             If an error occurs when sending the request.
	 * @throws FailedToCreateCacheException
	 *             If the Cache failed to create.
	 */
	public void createCache(String cacheName, CacheType type)
			throws CacheExistsException, IOException,
			FailedToCreateCacheException {
		if (exists(cacheName)) {
			throw new CacheExistsException(cacheName);
		}

		writeLine(MasterCacheletWorker.CREATECMD + " " + cacheName + " " + type);
		String response = readLine();
		if (!Boolean.parseBoolean(response)) {
			throw new FailedToCreateCacheException(cacheName);
		}
	}

	/**
	 * Sends a request to the Master service to destroy the Cache.
	 * 
	 * @param cacheName
	 *            The Cache to destroy.
	 * @throws IOException
	 *             If an error occurs when sending the request.
	 */
	public void destroyCache(String cacheName) throws IOException {
		writeLine(MasterCacheletWorker.DESTROYCMD + " " + cacheName);
	}
}