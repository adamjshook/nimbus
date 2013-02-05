package nimbus.client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import nimbus.server.TripleSetCacheletWorker;
import nimbus.utils.BigBitArray;
import nimbus.utils.ICacheletHash;
import nimbus.utils.Triple;
import nimbus.utils.DataZNodeWatcher;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.CacheInfo;
import nimbus.master.NimbusMaster;

public class NimbusTripleSetClient {

	private static final Logger LOG = Logger
			.getLogger(NimbusTripleSetClient.class);
	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	private HashMap<Integer, TripleSetCacheletConnection> list = new HashMap<Integer, TripleSetCacheletConnection>();
	private int numServers = -1;

	private HashSet<Integer> contains_set = new HashSet<Integer>();
	private TripleSetCacheletConnection tempConnection;
	private ICacheletHash cacheletHash = ICacheletHash.getInstance();
	private BigBitArray availabilityArray = null;
	private DataZNodeWatcher watcher = new DataZNodeWatcher();
	private Stat stat = new Stat();
	private int contains_numdown = 0;
	private String cacheName = null;
	private int replication;

	/**
	 * Initializes a connection to a Nimbus distributed triple set.
	 * 
	 * @param cacheName
	 *            The Cache to connect to.
	 * @throws CacheDoesNotExistException
	 * @throws IOException
	 *             If the Bloom filters do not exist.
	 */
	public NimbusTripleSetClient(String cacheName)
			throws CacheDoesNotExistException, IOException {
		this.cacheName = cacheName;
		this.replication = NimbusConf.getConf().getReplicationFactor();
		String[] cachelets = NimbusConf.getConf().getNimbusCacheletAddresses()
				.split(",");
		for (int i = 0; i < cachelets.length; ++i) {
			list.put(i,
					new TripleSetCacheletConnection(cacheName, cachelets[i]));
		}

		numServers = cachelets.length;

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
	 * @throws IOException
	 *             If the path is not absolute.
	 * @throws CacheDoesNotExistException
	 *             If the Cache does not exist.
	 */
	public void read(Path p) throws IOException, CacheDoesNotExistException {

		if (!p.isAbsolute()) {
			throw new IOException("Use absolute paths");
		}

		NimbusMaster.getInstance().getCacheInfoLock(cacheName);
		CacheInfo info = NimbusMaster.getInstance().getCacheInfo(cacheName);
		info.setFilename(p.toString());
		NimbusMaster.getInstance().setCacheInfo(cacheName, info);
		NimbusMaster.getInstance().releaseCacheInfoLock(cacheName);

		do {
			if (watcher.isTriggered()) {
				try {
					watcher.reset();
					info = new CacheInfo(Nimbus.getZooKeeper().getData(
							Nimbus.ROOT_ZNODE + "/" + cacheName, watcher, stat));
					availabilityArray = new BigBitArray(
							info.getAvailabilityArray());

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
				} catch (Exception e) {
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
	}

	/**
	 * Adds the given triple to the set
	 * 
	 * @param element
	 *            the Triple to add to the set
	 * @throws CacheDoesNotExistException
	 *             If the Cache does not exist.
	 */
	public boolean add(Triple element) throws CacheletsUnavailableException {

		checkWatch();

		contains_set.clear();
		cacheletHash.getCacheletsFromKey(element.toString(), contains_set,
				numServers, replication);

		contains_numdown = 0;
		for (Integer cacheletID : contains_set) {
			if (availabilityArray.isBitOn(cacheletID)) {
				tempConnection = list.get(cacheletID);
				try {
					return tempConnection.add(element);
				} catch (CacheletNotConnectedException e) {
					try {
						LOG.info("Caught CacheletNotConnectedException for ID "
								+ cacheletID + ".  Attempting reconnect...");
						tempConnection.connect();

						LOG.info("Successfully reconnected to ID " + cacheletID);
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
					tempConnection.disconnect();
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
	
	public Iterator<Triple> iterator() throws IOException {
		StreamingTripleSetIterator iter = new StreamingTripleSetIterator();
		for (int i = 0; i < numServers; ++i) {
			tempConnection = list.get(i);
			tempConnection.getAll();
			iter.addClient(tempConnection);
		}
		iter.initialize();
		return iter;
	}
	
	public Iterator<Triple> iterator(String s1) throws IOException {
		StreamingTripleSetIterator iter = new StreamingTripleSetIterator();
		for (int i = 0; i < numServers; ++i) {
			tempConnection = list.get(i);
			tempConnection.get(s1);
			iter.addClient(tempConnection);
		}
		iter.initialize();
		return iter;
	}

	public Iterator<Triple> iterator(String s1, String s2) throws IOException {
		StreamingTripleSetIterator iter = new StreamingTripleSetIterator();
		for (int i = 0; i < numServers; ++i) {
			tempConnection = list.get(i);
			tempConnection.get(s1, s2);
			iter.addClient(tempConnection);
		}
		iter.initialize();
		return iter;
	}

	/**
	 * Determines if a given element is a member of the triple set.
	 * 
	 * @param element
	 *            The element to test.
	 * @return Whether or not this element is a member of the triple set.
	 * @throws IOException
	 *             If an error occurs when communicating with the Cachelets
	 * @throws CacheletsUnavailableException
	 *             If all the Cachelets that would store the given element are
	 *             unavailable.
	 */
	public boolean contains(Triple element)
			throws CacheletsUnavailableException {

		checkWatch();

		contains_set.clear();
		cacheletHash.getCacheletsFromKey(element.toString(), contains_set,
				numServers, replication);

		contains_numdown = 0;
		for (Integer cacheletID : contains_set) {
			if (availabilityArray.isBitOn(cacheletID)) {
				tempConnection = list.get(cacheletID);
				try {
					return tempConnection.contains(element);
				} catch (CacheletNotConnectedException e) {
					try {
						LOG.info("Caught CacheletNotConnectedException for ID "
								+ cacheletID + ".  Attempting reconnect...");
						tempConnection.connect();

						LOG.info("Successfully reconnected to ID " + cacheletID);
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
					tempConnection.disconnect();
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
	 * Determines if a collection of Triples are all elements of this set.
	 * 
	 * @param c
	 *            The collection to test.
	 * @return True if all elements are members of the set, false otherwise.
	 * @throws IOException
	 */
	public boolean containsAll(Collection<Triple> c) throws IOException {
		for (Triple t : c) {
			if (!contains(t)) {
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

		checkWatch();

		for (TripleSetCacheletConnection set : list.values()) {
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
		for (TripleSetCacheletConnection worker : list.values()) {
			worker.disconnect();
		}
	}

	private void checkWatch() {
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Helper class to handle connections to each Cachelet. Used by the
	 * TripleSetClient to... well... connect to each Cachelet.
	 */
	private class TripleSetCacheletConnection extends BaseNimbusClient {

		private final Logger LOG = Logger
				.getLogger(TripleSetCacheletConnection.class);
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
		public TripleSetCacheletConnection(String cacheName, String cacheletName)
				throws CacheDoesNotExistException, IOException {
			super(cacheletName, NimbusMaster.getInstance().getCachePort(
					cacheName));
			super.cacheName = cacheName;
			this.cacheletName = cacheletName;
			connect();
		}

		/**
		 * Sends a request to the Cachelet to add the given element
		 * 
		 * @param element
		 *            The element to add.
		 * @return If the element was added.
		 * @throws IOException
		 */
		public boolean add(Triple element) throws IOException {
			writeLine(TripleSetCacheletWorker.ADD + " " + element.toString());
			String response = readLine();
			if (response.equals("true")) {
				return true;
			} else if (response.equals("false")) {
				return false;
			}

			throw new IOException("Did not receive a true or false response.");
		}

		public void getAll() throws IOException {
			writeLine(Integer.toString(TripleSetCacheletWorker.GET_ALL));
		}
		
		public void get(String s1) throws IOException {
			writeLine(TripleSetCacheletWorker.GET_WITH_ONE + " " + s1);
		}

		public void get(String s1, String s2) throws IOException {
			writeLine(TripleSetCacheletWorker.GET_WITH_TWO + " " + s1 + " "
					+ s2);
		}

		/**
		 * Sends a request to the Cachelet to determine if the given element is
		 * a member of the set.
		 * 
		 * @param element
		 *            The element to request.
		 * @return If the element is a member of the set.
		 * @throws IOException
		 */
		public boolean contains(Triple element) throws IOException {
			writeLine(TripleSetCacheletWorker.CONTAINS + " "
					+ element.toString());
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
			writeLine(Integer.toString(TripleSetCacheletWorker.ISEMPTY));
			String response = readLine();
			return Boolean.parseBoolean(response);
		}
	}
}