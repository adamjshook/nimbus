package nimbus.main;

import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import nimbus.client.BaseNimbusClient;
import nimbus.client.MasterClient;
import nimbus.master.CacheInfo;
import nimbus.master.NimbusMaster;
import nimbus.master.NimbusSafetyNet;
import nimbus.server.ICacheletServer;
import nimbus.server.CacheType;
import nimbus.server.SetCacheletServer;
import nimbus.server.MasterCacheletServer;
import nimbus.utils.BigBitArray;

/**
 * This is the main class for starting up a Cache. It parses command line
 * arguments and opens up a server based on the command line arguments to
 * receive requests from clients. The main class itself is responsible for
 * establishing connections between Cachelets themselves, as well as sending a
 * heartbeat to ZooKeeper.
 * 
 * See the PDF file located under $NIMBUS_HOME/doc for more information about
 * Nimbus, including overall design and features.
 */
public class Nimbus extends Configured implements Tool {

	public static final String ROOT_ZNODE = "/nimbus";
	public static String CACHE_ZNODE = null;
	public static String CACHELET_ZNODE = null;

	private static final Logger LOG = Logger.getLogger(Nimbus.class);
	private static CacheInfo info = null;
	private static Random rndm = new Random();
	private static final byte[] EMPTY_DATA = "".getBytes();
	private static ZooKeeper s_zk = null;

	private static String cacheName;
	private static int port;
	private static CacheType type;

	// Options
	private Options options = null;
	private CommandLineParser parser = new PosixParser();
	private CommandLine line;
	private Map<String, BaseNimbusClient> knownServers = new HashMap<String, BaseNimbusClient>();
	private ICacheletServer cachelet;

	@Override
	public int run(String[] args) throws Exception {
		parseOptions(args);

		if (line.hasOption("start")) {
			startCache();
		} else if (line.hasOption("kill")) {
			killCache(line.getOptionValue("kill"));
		} else {
			throw new InvalidParameterException("Unknown mode to run in.");
		}

		return 0;
	}

	private void killCache(String name) throws IOException {

		MasterClient master = new MasterClient();
		master.destroyCache(name);
	}

	private void startCache() throws Exception {

		LOG.info("Starting Nimbus cache");
		Nimbus.getZooKeeper();

		// Set the Cache ZNode to the root + the Cache name
		CACHE_ZNODE = ROOT_ZNODE + "/" + cacheName;
		String cacheletName = InetAddress.getLocalHost().getHostName();
		CACHELET_ZNODE = CACHE_ZNODE + "/" + cacheletName;
		knownServers.put(InetAddress.getLocalHost().getHostName(), null);

		// add shutdown hook
		NimbusShutdownHook.createInstance(type);

		// check if the root node exists, if it doesn't create it
		if (getZooKeeper().exists(ROOT_ZNODE, false) == null) {
			LOG.info("Creating root Znode");
			getZooKeeper().create(ROOT_ZNODE, EMPTY_DATA, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}

		// Fire up the safety net if enabled
		if (type.equals(CacheType.MASTER)) {
			// check if the Cache node exists, if it doesn't create it
			NimbusMaster.getInstance().getCacheInfoLock(cacheName);
			info = NimbusMaster.getInstance().getCacheInfo(cacheName);
			if (info == null) {
				LOG.info("CacheInfo is null.  Creating CacheZNode...");
				info = new CacheInfo();
				info.setName(cacheName);
				info.setType(type);
				info.setPort(port);
				BigBitArray array = new BigBitArray(
						BigBitArray.makeMultipleOfEight(NimbusConf.getConf()
								.getNumNimbusCachelets()));
				info.setAvailabilityArray(array.getBytes());

				getZooKeeper().create(CACHE_ZNODE,
						info.getByteRepresentation(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				LOG.info("Creating Cache ZNode at " + CACHE_ZNODE
						+ " with data of size "
						+ info.getByteRepresentation().length);
			}
			NimbusMaster.getInstance().releaseCacheInfoLock(cacheName);

			if (NimbusConf.getConf().isSafetyNetEnabled()) {
				// Fire up the Safety Net
				Thread safetynet = new Thread(NimbusSafetyNet.getInstance());
				safetynet.start();
			}
		} else {
			// this info is for a non-master cache
			info = NimbusMaster.getInstance().getCacheInfo(cacheName);
			if (info == null) {
				throw new RuntimeException("No info for " + cacheName);
			}
		}

		// create my Cachelet
		switch (info.getType()) {
		case DISTRIBUTED_SET:
			cachelet = new SetCacheletServer(info.getName(), cacheletName,
					info.getPort(), info.getType());
			break;
		case MASTER:
			cachelet = new MasterCacheletServer(info.getName(), cacheletName,
					info.getPort(), info.getType());
			break;
		}

		Thread t = new Thread(cachelet);
		t.start();

		// add myself to ZooKeeper
		if (getZooKeeper().exists(CACHELET_ZNODE, false) != null) {
			LOG.info("My ZNode exists for some reason... Deleting old ZNode.");
			getZooKeeper().delete(CACHELET_ZNODE, -1);
		}

		// Create my ZNode
		LOG.info("Creating my ZNode at " + CACHELET_ZNODE);
		getZooKeeper().create(CACHELET_ZNODE, EMPTY_DATA, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);

		// this while loop manages connections to other Cachelets
		// if a Cachelet connects, then create a new thread to handle
		// communication to that Cachelet and wait for more connections

		LOG.info("Starting heartbeat cycle...");
		long hbInterval = NimbusConf.getConf().getCacheletHeartbeatInterval();
		getZooKeeper().setData(CACHELET_ZNODE, EMPTY_DATA, -1);
		long lastheartbeat = System.currentTimeMillis(), now;
		while (!false) {

			// heartbeat for Cachelet
			now = System.currentTimeMillis();
			if (now - lastheartbeat >= hbInterval) {
				getZooKeeper().setData(CACHELET_ZNODE, EMPTY_DATA, -1);
				lastheartbeat = now;
			}
		}
	}

	/**
	 * Returns a random Nimbus Server machine address.
	 * 
	 * @return A random Nimbus Server machine address.
	 */
	public static String getRandomNimbusHost() {
		String[] hosts = NimbusConf.getConf().getNimbusCacheletAddresses()
				.split(",");
		return hosts[Math.abs(rndm.nextInt()) % hosts.length];
	}

	/**
	 * Returns the ZooKeeper instance.
	 * 
	 * @return The ZooKeeper instance.
	 */
	public static ZooKeeper getZooKeeper() {
		if (s_zk == null) {
			// Create ZK Instance
			try {
				LOG.info("Connecting to ZooKeeper at "
						+ NimbusConf.getConf().getZooKeeperServers());
				s_zk = new ZooKeeper(
						NimbusConf.getConf().getZooKeeperServers(),
						Integer.MAX_VALUE, new Watcher() {
							@Override
							public void process(WatchedEvent event) {
							}
						});
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("Failed to initialize ZooKeeper");
				System.exit(-1);
			}
		}

		return s_zk;
	}

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new Configuration(), new Nimbus(), args));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// ///////////
	// OPTIONS //
	// ///////////

	@SuppressWarnings("static-access")
	private Options getOptions() {
		if (options == null) {
			options = new Options();

			options.addOption(OptionBuilder.withLongOpt("start")
					.withDescription("Start a cache").create('s'));

			options.addOption(OptionBuilder.withLongOpt("kill").hasArg()
					.withDescription("Kill a cache").create('s'));

			options.addOption(OptionBuilder.withLongOpt("port").hasArg()
					.withDescription("Port to initialize Nimbus with.")
					.create('p'));
			options.addOption(OptionBuilder.withLongOpt("name").hasArg()
					.withDescription("Name of this Cache.").create('n'));
			options.addOption(OptionBuilder.withLongOpt("type").hasArg()
					.withDescription("Cache type.").create('t'));

			options.addOption(OptionBuilder.withLongOpt("help")
					.withDescription("Displays this help message.").create());
		}
		return options;
	}

	private void parseOptions(String[] args) {

		if (args.length == 0) {
			printHelp();
			System.exit(0);
		}

		try {
			line = parser.parse(getOptions(), args);

			if (line.hasOption("start") && line.hasOption("kill")) {
				throw new ParseException(
						"Cannot simultaneously start and kill a cache");
			}

			// verify all required options are there
			if (line.hasOption("start")
					&& (!line.hasOption("port") || !line.hasOption("name") || !line
							.hasOption("type"))) {
				throw new ParseException(
						"Cannot start cache without port, name, and type params");
			}
		} catch (MissingOptionException e) {
			System.err.println(e.getMessage());
			printHelp();
			System.exit(-1);
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		if (line.hasOption("help")) {
			printHelp();
			System.exit(0);
		}

		if (line.hasOption("start")) {
			cacheName = line.getOptionValue("name");
			type = CacheType.valueOf(line.getOptionValue("type").toUpperCase());
			port = Integer.parseInt(line.getOptionValue("port"));

			if (type == null) {
				LOG.error("Invalid type.");
				printHelp();
				System.exit(-1);
			}
		}
	}

	private void printHelp() {
		HelpFormatter help = new HelpFormatter();
		help.printHelp("java -jar $NIMBUS_HOME/bin/nimbus.jar [opts]",
				"Command Line Arguments", getOptions(), "");
	}
}
