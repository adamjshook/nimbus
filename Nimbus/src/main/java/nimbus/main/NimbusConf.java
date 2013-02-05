package nimbus.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import nimbus.utils.CacheletHashType;

/**
 * This class is designed to handle all Nimbus configuration parameters. The
 * default file is loaded based off the $NIMBUS_HOME environment variable, and
 * is located at $NIMBUS_HOME/conf/nimbus-default.xml.<br>
 * <br>
 * 
 * It follows a Singleton paradigm and has helper functions to retrieve
 * configuration values.
 */
public class NimbusConf extends Configuration {

	private static final Logger LOG = Logger.getLogger(NimbusConf.class);
	static {
		LOG.setLevel(Level.toLevel(NimbusConf.getConf().get(
				NimbusConf.LOG4J_LEVEL_CONF_VAR, "INFO")));
	}

	public static final String NIMBUS_HOME_CONF_VAR = "nimbus.home";
	public static final String NIMBUS_MASTER_PORT_CONF_VAR = "nimbus.master.port";
	public static final String NIMBUS_PORTS_VAR = "nimbus.ports";
	public static final String NIMBUS_SERVER_CONF_VAR = "nimbus.servers";
	public static final String FS_DEFAULT_NAME_CONF_VAR = "fs.default.name";
	public static final String JAVA_HOME_CONF_VAR = "java.home";
	public static final String LOG4J_LEVEL_CONF_VAR = "log4j.level";
	public static final String ZK_SERVERS_CONF_VAR = "zookeeper.quorum.servers";
	public static final String NIMBUS_NUM_SERVERS_CONF_VAR = "nimbus.num.servers";
	public static final String NIMBUS_JAVA_OPTS = "nimbus.java.opts";
	public static final String SERVER_HASH_TYPE = "nimbus.server.hash";
	public static final String NIMBUS_SAFETY_NET_TIMEOUT = "nimbus.safety.net.timeout";
	public static final String NIMBUS_SAFETY_NET_ENABLED = "nimbus.safety.net.enabled";
	public static final String NIMBUS_CACHELET_HEARTBEAT = "nimbus.cachelet.heartbeat";
	public static final String NIMBUS_REPLICATION_FACTOR = "nimbus.replication.factor";

	private static NimbusConf s_instance = null;

	public static NimbusConf getConf() {
		if (s_instance == null) {
			loadConfiguration();

			if (s_instance.isSafetyNetEnabled()
					&& s_instance.getCacheletHeartbeatInterval() >= s_instance
							.getSafetyNetTimeout()) {
				throw new RuntimeException(
						"Error: Cachelet heartbeat interval is greater than the safety net timeout.");
			}
		}
		return s_instance;
	}

	private NimbusConf() {
	}

	public String getNimbusHomeDir() {
		return super.get(NIMBUS_HOME_CONF_VAR);
	}

	public String getNimbusMasterPort() {
		return super.get(NIMBUS_MASTER_PORT_CONF_VAR);
	}

	public String getNimbusCacheletPortRange() {
		return super.get(NIMBUS_PORTS_VAR);
	}

	public String getNimbusCacheletAddresses() {
		return super.get(NIMBUS_SERVER_CONF_VAR);
	}

	public String getFSDefaultName() {
		return super.get(FS_DEFAULT_NAME_CONF_VAR);
	}

	public String getJavaHomeDir() {
		String home = super.get(JAVA_HOME_CONF_VAR);
		if (home == null) {
			throw new RuntimeException(
					"java.home property not set in $NIMBUS_HOME/conf/nimbus-default.xml");
		}
		return home;
	}

	public Level getLog4JLevel() {
		return Level.toLevel(super.get(LOG4J_LEVEL_CONF_VAR));
	}

	public int getNumNimbusCachelets() {
		return Integer.parseInt(super.get(NIMBUS_NUM_SERVERS_CONF_VAR));
	}

	public String getZooKeeperServers() {
		return super.get(ZK_SERVERS_CONF_VAR);
	}

	public String getJavaOpts() {
		return super.get(NIMBUS_JAVA_OPTS);
	}

	public CacheletHashType getCacheletHashType() {
		return CacheletHashType.valueOf(super.get(SERVER_HASH_TYPE));
	}

	public long getSafetyNetTimeout() {
		return Long.parseLong(super.get(NIMBUS_SAFETY_NET_TIMEOUT));
	}

	public long getCacheletHeartbeatInterval() {
		return Long.parseLong(super.get(NIMBUS_CACHELET_HEARTBEAT));
	}

	public boolean isSafetyNetEnabled() {
		return Boolean.parseBoolean(super.get(NIMBUS_SAFETY_NET_ENABLED));
	}

	public int getReplicationFactor() {
		return Integer.parseInt(super.get(NIMBUS_REPLICATION_FACTOR));
	}

	private static void loadConfiguration() {
		try {
			
			if (System.getenv("NIMBUS_HOME") == null) {
				throw new RuntimeException("NIMBUS_HOME environment variable not set");
			}
			
			s_instance = new NimbusConf();

			s_instance.addResource(new Path(System.getenv("NIMBUS_HOME")
					+ "/conf/nimbus-default.xml"));
			String servers = "";

			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					new FileInputStream(System.getenv("NIMBUS_HOME")
							+ "/conf/servers")));

			String tmp;
			int numcachelets = 0;
			while ((tmp = rdr.readLine()) != null) {
				servers += tmp + ",";
				++numcachelets;
			}

			rdr.close();

			s_instance.set(NIMBUS_SERVER_CONF_VAR, servers.substring(0, servers
					.length() - 1));
			s_instance.setInt(NIMBUS_NUM_SERVERS_CONF_VAR, numcachelets);
		} catch (FileNotFoundException e) {
			LOG.error("Failed to find configuration file.");
			LOG.error(e.getMessage());
			System.exit(-1);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			System.exit(-1);
		}
	}
}
