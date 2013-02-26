package nimbus.main;

import java.util.List;

import nimbus.master.NimbusSafetyNet;
import nimbus.server.CacheType;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * This class handles graceful shutdown of Nimbus. Its mainly deletes this
 * machine's ZNode from ZooKeeper, as well as deleting the Cache ZNode if no
 * more children exist.<br>
 * <br>
 * 
 * Forceful killing of a Nimbus process will leave this machine's ZNode in
 * Nimbus, and will therefore corrupt ZooKeeper.
 */
public class NimbusShutdownHook extends Thread {

	private static final Logger LOG = Logger
			.getLogger(NimbusShutdownHook.class);

	private static NimbusShutdownHook s_instance = null;
	private boolean clean = false;
	private CacheType type = null;

	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public static void createInstance(CacheType type) {
		if (s_instance == null) {
			s_instance = new NimbusShutdownHook(type);
			Runtime.getRuntime().addShutdownHook(s_instance);
		}
	}

	public static NimbusShutdownHook getInstance() {
		return s_instance;
	}

	private NimbusShutdownHook(CacheType type) {
		this.type = type;
	}

	public void cleanShutdown() {
		this.clean = true;
	}

	/**
	 * This method is executed when Nimbus is shutdown to cleanup ZNodes in
	 * ZooKeeper.
	 */
	@Override
	public void run() {
		LOG.info("Shutting down server.  You stay classy Sandy Eggo.");
		try {

			if (type.equals(CacheType.MASTER)) {
				LOG.info("Stopping safety net...");
				NimbusSafetyNet.getInstance().stop();
			}

			if (clean) {
				LOG.info("Clean shutdown of this cache");
				if (Nimbus.getZooKeeper().exists(Nimbus.CACHELET_ZNODE, false) != null) {
					LOG.info("Deleting my ZNode: " + Nimbus.CACHELET_ZNODE);
					Nimbus.getZooKeeper().delete(Nimbus.CACHELET_ZNODE, -1);
				}

				List<String> children = Nimbus.getZooKeeper().getChildren(
						Nimbus.CACHE_ZNODE, false);
				if (children.size() == 0) {
					LOG.info("No more children left.  Deleting Cache node: "
							+ Nimbus.CACHE_ZNODE);
					Nimbus.getZooKeeper().delete(Nimbus.CACHE_ZNODE, -1);
				}
			} else {
				LOG.info("Not a clean shutdown.  Leaving ZNodes");
			}

			Nimbus.getZooKeeper().close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			if (!e.code().equals(Code.NONODE)) {
				e.printStackTrace();
			}
		}
	}
}
