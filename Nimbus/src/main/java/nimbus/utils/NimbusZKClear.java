package nimbus.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import nimbus.main.Nimbus;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


/**
 * A utility to clear all ZNodes related to ZooKeeper. Was mainly used in
 * development. Can be accessed through the command line via<br>
 * <tt> java -classpath $NIMBUS_HOME/bin/nimbus.jar nimbus.utils.NimbusZKClear</tt>
 * or through the static clear function.<br>
 * <br>
 * Clearing ZooKeeper while Caches or the Master will cause unknown errors.
 */
public class NimbusZKClear {

	public static void main(String[] args) throws InterruptedException,
			KeeperException, IOException {

		System.out.println("This utility clears all ZNodes used by Nimbus.");
		System.out
				.println("This should really only be used when restarting Nimbus to ensure all ZNodes are cleared.");
		System.out.println("It was mainly created for development purposes.");
		System.out
				.println("Are you sure you want to clear all ZNodes for Nimbus?");
		System.out
				.print("Type CLEAR (case sensitive) to clear, or anything else to quit.\n\n>>>");
		BufferedReader rdr = new BufferedReader(
				new InputStreamReader(System.in));

		String answer = rdr.readLine();

		if (answer.equals("CLEAR")) {
			System.out.println("Clearing Nimbus ZNodes...  ");
			if (NimbusZKClear.clear()) {
				System.out.println("... Done.");
			} else {
				System.out.println("Failed to clear Nimbus ZNodes.");
			}
		} else {
			System.out.println("Exiting without clearing Nimbus ZNodes");
		}

		System.exit(0);
	}

	/**
	 * Clears all ZNodes from ZooKeeper related to Nimbus, including the Nimbus
	 * root ZNode.
	 */
	public static boolean clear() {
		try {
			ZooKeeper zk = Nimbus.getZooKeeper();
			if (zk.exists(Nimbus.ROOT_ZNODE, false) != null) {
				clearHelper(Nimbus.ROOT_ZNODE, zk);
			}

			zk.close();
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Recursive helper function to clear ZooKeeper.
	 * @throws KeeperException If a ZooKeeper related error occurs.
	 * @throws InterruptedException If an error occurs when communicated with the ZooKeeper server.
	 */
	private static void clearHelper(String path, ZooKeeper zk)
			throws KeeperException, InterruptedException {

		List<String> children = zk.getChildren(path, false);
		for (String child : children) {
			clearHelper(path + "/" + child, zk);
		}

		System.out.println("*** Deleting " + path + " ***");
		zk.delete(path, -1);
	}
}
