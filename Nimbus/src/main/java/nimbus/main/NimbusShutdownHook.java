package nimbus.main;

import java.util.List;

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
	static {
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	/**
	 * This method is executed when Nimbus is shutdown to cleanup ZNodes in
	 * ZooKeeper.
	 */
	@Override
	public void run() {
		LOG.info("Shutting down server.  You stay classy Sandy Eggo.");
		try {
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
