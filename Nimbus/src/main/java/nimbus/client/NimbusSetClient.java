package nimbus.client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import nimbus.server.SetCacheletServer;
import nimbus.server.SetCacheletWorker;
import nimbus.utils.BigBitArray;
import nimbus.utils.BloomFilter;
import nimbus.utils.DataZNodeWatcher;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.CacheInfo;
import nimbus.master.NimbusMaster;
import nimbus.utils.ICacheletHash;

public class NimbusSetClient {

	private static final Logger LOG = Logger.getLogger(NimbusSetClient.class);
	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	private HashMap<Integer, DSetCacheletConnection> list = new HashMap<Integer, DSetCacheletConnection>();
	private HashMap<Integer, BloomFilter> filters = new HashMap<Integer, BloomFilter>();
	private int numServers = -1;

	private HashSet<Integer> contains_set = new HashSet<Integer>();
	private ICacheletHash cacheletHash = ICacheletHash.getInstance();
	private BigBitArray availabilityArray = null;
	private DataZNodeWatcher watcher = new DataZNodeWatcher();
	private Stat stat = new Stat();
	private int contains_numdown = 0;
	private String cacheName = null;
	private int replication;

	/**
	 * Initializes a connection to a Nimbus distributed set. Downloads the Bloom
	 * filters by default.<br>
	 * <br>
	 * Use {@link NimbusSetClient#DSetClient(String, boolean)} to determine this
	 * behavior.
	 * 
	 * @param cacheName
	 *            The Cache to connect to.
	 * @throws CacheDoesNotExistException
	 * @throws IOException
	 */
	public NimbusSetClient(String cacheName) throws CacheDoesNotExistException,
			IOException {
		this(cacheName, true);
	}

	/**
	 * Initializes a connection to a Nimbus distributed set. Downloads the Bloom
	 * filters based on the given parameter.
	 * 
	 * @param cacheName
	 *            The Cache to connect to.
	 * @param download
	 *            Whether or not to download the Bloom filters for this Cache.
	 *            Typically, you won't want to download the Bloom filters if you
	 *            intent to use this client instance to ingest into the Cache.
	 * @throws CacheDoesNotExistException
	 * @throws IOException
	 *             If the Bloom filters do not exist.
	 */
	public NimbusSetClient(String cacheName, boolean download)
			throws CacheDoesNotExistException, IOException {
		this.cacheName = cacheName;
		this.replication = NimbusConf.getConf().getReplicationFactor();
		String[] cachelets = NimbusConf.getConf().getNimbusCacheletAddresses()
				.split(",");
		for (int i = 0; i < cachelets.length; ++i) {
			list.put(i, new DSetCacheletConnection(cacheName, cachelets[i]));
		}

		numServers = cachelets.length;

		if (download) {
			downloadBFilters();
		}

		try {
			CacheInfo info = new CacheInfo(Nimbus.getZooKeeper().getData(
					Nimbus.ROOT_ZNODE + "/" + cacheName, watcher, stat));
			availabilityArray = new BigBitArray(info.getAvailabilityArray());
		} catch (KeeperException e) {
			e.printStackTrace();
			throw new RuntimeException("ZooKeeper error.");
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException("ZooKeeper error.");
		}
	}

	/**
	 * Ingests the given absolute path into the Cache based on the given
	 * parameters.
	 * 
	 * @param p
	 *            The file to ingest. Each line in the file is considered a
	 *            record. The path must be an absolute path.
	 * @param approxNumRecords
	 *            The approximate number of records inside the file.
	 * @param falsePosRate
	 *            The desired false positive rate.
	 * @param download
	 *            Whether or not to download the Bloom filters. Typically,
	 *            you'll want to download the filters if you intend to use this
	 *            same client to check if an element is a member of the set.
	 * @throws IOException
	 *             If the path is not absolute.
	 * @throws CacheDoesNotExistException
	 *             If the Cache does not exist.
	 */
	public void read(Path p, int approxNumRecords, float falsePosRate,
			boolean download) throws IOException, CacheDoesNotExistException {

		if (!p.isAbsolute()) {
			throw new IOException("Use absolute paths");
		}

		NimbusMaster.getInstance().getCacheInfoLock(cacheName);
		CacheInfo info = NimbusMaster.getInstance().getCacheInfo(cacheName);
		info.setApproxNumRecords(approxNumRecords);
		info.setFilename(p.toString());
		info.setFalsePosRate(falsePosRate);
		NimbusMaster.getInstance().setCacheInfo(cacheName, info);
		NimbusMaster.getInstance().releaseCacheInfoLock(cacheName);
		
		/*
		 * for (int i = 0; i < list.size(); ++i) { list.get(i).read(p,
		 * approxNumRecords, falsePosRate, i); }
		 */

		do {
			if (watcher.isTriggered()) {
				try {
					watcher.reset();
					info = new CacheInfo(Nimbus.getZooKeeper().getData(
							Nimbus.ROOT_ZNODE + "/" + cacheName, watcher, stat));
					availabilityArray = new BigBitArray(info
							.getAvailabilityArray());

					int numon = 0;
					for (int i = 0; i < list.values().size(); ++i) {
						if (availabilityArray.isBitOn(i)) {
							++numon;
						}
					}

					if (numon >= NimbusConf.getConf().getNumNimbusCachelets()
							- (NimbusConf.getConf().getReplicationFactor() - 1)) {
						break;
					}
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} while (true);

		/*
		 * for (DSetCacheletConnection cachelet : list.values()) { if
		 * (!Boolean.parseBoolean(cachelet.readLine())) { throw new
		 * IOException("Failed to read file."); } }
		 */

		if (download) {
			downloadBFilters();
		}
	}

	/*
	/**
	 * Clears this set of all elements.
	 * 
	 * @throws IOException
	 *
	public void clear() throws IOException {
		for (DSetCacheletConnection set : list.values()) {
			set.clear();
		}
	}
	*/
	/**
	 * Determines if a given element is a member of the set.
	 * 
	 * Checks the Bloom filters and if it responds 'maybe', this client will
	 * then send a request to the Cachelet.
	 * 
	 * @param element
	 *            The element to test.
	 * @return Whether or not this element is a member of the set.
	 * @throws IOException
	 *             If an error occurs when communicating with the Cachelets
	 * @throws CacheletsUnavailableException
	 *             If all the Cachelets that would store the given element are
	 *             unavailable.
	 */
	private DSetCacheletConnection contains_connect_tmp;

	public boolean contains(String element)
			throws CacheletsUnavailableException {

		if (watcher.isTriggered()) {
			try {
				watcher.reset();
				CacheInfo info = new CacheInfo(Nimbus.getZooKeeper().getData(
						Nimbus.ROOT_ZNODE + "/" + cacheName, watcher, stat));
				availabilityArray = new BigBitArray(info.getAvailabilityArray());

				for (int i = 0; i < list.values().size(); ++i) {
					if (!availabilityArray.isBitOn(i)
							&& list.get(i).isConnected()) {
						list.get(i).disconnect();
						LOG.info("Disconnecting " + i
								+ " due to Watch triggered.");
					}
				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		contains_set.clear();
		cacheletHash.getCacheletsFromKey(element, contains_set, numServers,
				replication);

		contains_numdown = 0;
		for (Integer cacheletID : contains_set) {
			if (availabilityArray.isBitOn(cacheletID)) {
				contains_connect_tmp = list.get(cacheletID);
				try {
					if (filters.get(cacheletID).membershipTest(element)) {
						if (contains_connect_tmp.contains(element)) {
							return true;
						}
					}
				} catch (CacheletNotConnectedException e) {
					try {
						LOG.info("Caught CacheletNotConnectedException for ID "
								+ cacheletID + ".  Attempting reconnect...");
						contains_connect_tmp.connect();
						filters.put(cacheletID, contains_connect_tmp
								.getBloomFilter());
						LOG
								.info("Successfully reconnected to ID "
										+ cacheletID);
						if (++contains_numdown == replication) {
							throw new CacheletsUnavailableException();
						}
					} catch (IOException e1) {
						e1.printStackTrace();
						if (++contains_numdown == replication) {
							throw new CacheletsUnavailableException();
						}
					}
				} catch (IOException e) {
					LOG.error("Received error from Cachelet ID " + cacheletID
							+ ": " + e.getMessage());
					LOG.error("Disconnecting.");
					contains_connect_tmp.disconnect();
					availabilityArray.set(cacheletID, false);
					if (++contains_numdown == replication) {
						throw new CacheletsUnavailableException();
					}
				}
			} else {
				if (++contains_numdown == replication) {
					throw new CacheletsUnavailableException();
				}
			}
		}

		return false;
	}

	/**
	 * Determines if a collection of strings are all elements of this set.
	 * 
	 * @param c
	 *            The collection to test.
	 * @return True if all elements are members of the set, false otherwise.
	 * @throws IOException
	 */
	public boolean containsAll(Collection<String> c) throws IOException {
		for (String o : c) {
			if (!contains(o)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Determines whether or not this Cache is empty.
	 * 
	 * @return True if the Cache is empty, false otherwise.
	 * @throws IOException
	 */
	public boolean isEmpty() throws IOException {
		for (DSetCacheletConnection set : list.values()) {
			if (!set.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Disconnects this set from all Cachelets.
	 */
	public void disconnect() {
		for (DSetCacheletConnection worker : list.values()) {
			worker.disconnect();
		}
	}

	/**
	 * Downloads the Bloom filters off of HDFS and stores them in memory for
	 * this Client.
	 * 
	 * @throws IOException
	 */
	private void downloadBFilters() throws IOException {
		try {
			filters.clear();
			long start = System.currentTimeMillis();
			for (Entry<Integer, DSetCacheletConnection> e : list.entrySet()) {
				filters.put(e.getKey(), e.getValue().getBloomFilter());
			}
			LOG.info("Done downloading Bloom filters... Took "
					+ (System.currentTimeMillis() - start) + " ms.");
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw new IOException("Failed to download Bloom filter.");
		}
	}

	/**
	 * Helper class to handle connections to each Cachelet. Used by the
	 * DSetClient to... well... connect to each Cachelet.
	 */
	private class DSetCacheletConnection extends BaseNimbusClient {

		private final Logger LOG = Logger
				.getLogger(DSetCacheletConnection.class);
		private String cacheletName;

		/**
		 * Connects to the given host. Automatically gets the port from the
		 * Master based on the given Cache name.
		 * 
		 * @param cacheName
		 *            The Cache to connect to.
		 * @param cacheletName
		 *            The host of the Cachelet.
		 * @throws CacheDoesNotExistException
		 *             If the Cache does not exist.
		 * @throws IOException
		 *             If some bad juju happens.
		 */
		public DSetCacheletConnection(String cacheName, String cacheletName)
				throws CacheDoesNotExistException, IOException {
			super(cacheletName, NimbusMaster.getInstance().getCachePort(
					cacheName));
			super.cacheName = cacheName;
			this.cacheletName = cacheletName;
			connect();
		}
		
		/*
		/**
		 * Clears this Cachelet of all elements.
		 * 
		 * @throws IOException
		 *             If an error occurs when sending the request.
		 *
		public void clear() throws IOException {
			writeLine(Integer.toString(DSetCacheletWorker.CLEAR));
			dumpResponses();
		}
		*/

		/**
		 * Sends a request to the Cachelet to determine if the given element is
		 * a member of the set.
		 * 
		 * @param element
		 *            The element to request.
		 * @return If the element is a member of the set.
		 * @throws IOException
		 */
		public boolean contains(String element) throws IOException {
			writeLine(SetCacheletWorker.CONTAINS + " " + element);
			String response = readLine();
			if (response.equals("true")) {
				return true;
			} else if (response.equals("false")) {
				return false;
			}

			throw new IOException("Did not receive a true or false response.");
		}

		/**
		 * Sends a request to the Cachelet to determine if this Cachelet has any
		 * elements.
		 * 
		 * @return If the Cachelet is empty.
		 * @throws IOException
		 *             If an error occurs when sending the request.
		 */
		public boolean isEmpty() throws IOException {
			writeLine(Integer.toString(SetCacheletWorker.ISEMPTY));
			String response = readLine();
			return Boolean.parseBoolean(response);
		}

		/**
		 * Deserializes the Bloom filter from the given Path and returns it.
		 * 
		 * @param p
		 *            The path to get the file from.
		 * @return The deserialized Bloom filter object.
		 * @throws IOException
		 *             If the Path is not a file or an error occurs when
		 *             deserializing the filter.
		 */
		public BloomFilter getBloomFilter() throws IOException {
			BloomFilter bf = new BloomFilter(SetCacheletServer
					.getBloomFilterPath(cacheName, cacheletName));
			if (LOG.isDebugEnabled()) {
				LOG.debug(bf.toString());
			}
			return bf;
		}
	}
}