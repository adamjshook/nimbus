package nimbus.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import nimbus.master.CacheInfo;
import nimbus.master.NimbusMaster;
import nimbus.utils.BloomFilter;
import nimbus.utils.CSet;
import nimbus.utils.ICacheletHash;

public class SetCacheletServer extends ICacheletServer {

	private CSet set = CSet.getInstance();
	private static BloomFilter bfilter = null;
	private static final Logger LOG = Logger
			.getLogger(SetCacheletServer.class);

	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public static Path getBloomFilterPath(String cacheName, String cacheletName) {
		return new Path(Nimbus.ROOT_ZNODE + "/" + cacheName + "/"
				+ cacheletName);
	}

	@Override
	protected ICacheletWorker getNewWorker() {
		return new SetCacheletWorker(this);
	}

	@Override
	public void run() {
		openServer();

		CacheInfo info = NimbusMaster.getInstance().getCacheInfo(cacheName);
		if (info.getFilename() != null) {
			LOG.info("Re-ingesting " + info.getFilename() + "...");
			this.distributedLoadFromHDFS(new Path(info.getFilename()), info
					.getApproxNumRecords(), info.getFalsePosRate(),
					NimbusMaster.getInstance().getCacheletID(cacheletName));
		} else { // leave watch on node for when it does change.
			try {
				Nimbus.getZooKeeper().getData(Nimbus.CACHE_ZNODE, new CacheletDataWatcher(this), null);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		acceptConnections();
	}

	public SetCacheletServer(String cacheName, String cacheletName, int port,
			CacheType type) {
		super(cacheName, cacheletName, port, type);
	}

	public void clear() {
		set.clear();
	}

	public boolean contains(String element) {
		return set.contains(element);
	}

	public boolean isEmpty() {
		return set.isEmpty();
	}

	public boolean distributedLoadFromHDFS(Path file, int approxNumRecords,
			float desiredFalsePosRate, int cacheletID) {
		LOG.info("distributedLoadFromHDFS:: " + file + " " + approxNumRecords + " " + desiredFalsePosRate + " " + cacheletID);
		int numCachelets = NimbusConf.getConf().getNumNimbusCachelets();
		int replication = NimbusConf.getConf().getReplicationFactor();;
		approxNumRecords = approxNumRecords / numCachelets * replication;
		bfilter = new BloomFilter(approxNumRecords, desiredFalsePosRate);
		try {
			NimbusMaster.getInstance().setCacheletAvailability(cacheName,
					cacheletName, false);
			
			FileSystem fs = FileSystem.get(NimbusConf.getConf());
			long start = System.currentTimeMillis();
			
			LOG.info("Reading from file " + file.makeQualified(fs)
					+ ".  My Cachelet ID is " + cacheletID);

			ICacheletHash hashalgo = ICacheletHash.getInstance();
			if (hashalgo == null) {
				throw new RuntimeException("Hash algorithm is null");
			}

			// open the file for read.
			BufferedReader rdr = new BufferedReader(new InputStreamReader(fs
					.open(file)));
			int numrecords = 0, added = 0;
			String s;
			while ((s = rdr.readLine()) != null) {
				++numrecords;
				if (hashalgo.isValidCachelet(cacheletID, s, numCachelets,
						replication)) {
					bfilter.train(s);
					set.add(s);
					++added;
				}
			}
			rdr.close();

			LOG.info("Num records: " + numrecords + "  Added: " + added
					+ "  Took " + (System.currentTimeMillis() - start) + "ms.");

			System.gc();

			serializeBloom();

			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			NimbusMaster.getInstance().setCacheletAvailability(cacheName,
					cacheletName, true);
		}
	}

	private boolean serializeBloom() {
		if (bfilter == null) {
			throw new NullPointerException(
					"Bloom filter is null. Call initBloomFilter.");
		}

		try {
			bfilter.serialize(getBloomFilterPath(cacheName, cacheletName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		bfilter.clear();
		bfilter = null;
		return true;
	}
	
	private class CacheletDataWatcher implements Watcher {

		private SetCacheletServer server = null;
		
		public CacheletDataWatcher(SetCacheletServer server) {
			this.server = server;
		}
		
		@Override
		public void process(WatchedEvent event) {
			if (event.getType().equals(EventType.NodeDataChanged)) {
				CacheInfo info = NimbusMaster.getInstance().getCacheInfo(cacheName);
				if (info.getFilename() != null) {
					LOG.info("Data Watch Trigger.  Ingesting file " + info.getFilename() + "...");
					server.distributedLoadFromHDFS(new Path(info.getFilename()), info
							.getApproxNumRecords(), info.getFalsePosRate(),
							NimbusMaster.getInstance().getCacheletID(cacheletName));
				}
			}
		}
	}
}