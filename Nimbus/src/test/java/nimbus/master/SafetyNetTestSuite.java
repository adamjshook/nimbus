package nimbus.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import nimbus.main.Nimbus;
import nimbus.main.NimbusConf;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import nimbus.utils.NimbusZKClear;

public class SafetyNetTestSuite implements ISafetyNetListener {
	private boolean notifiedcacheletadded = false,
			notifiedcacheletremoved = false;
	private boolean notifiedcacheadded = false, notifiedcacheremoved = false;
	private boolean notifiedcachestale = false;

	@Before
	public void setup() throws IOException, KeeperException,
			InterruptedException {
		System.out.println("setupcalled");
		NimbusZKClear.clear();
		Thread.sleep(1000);
		try {
			Nimbus.getZooKeeper().create(Nimbus.ROOT_ZNODE, "".getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (!e.code().equals(Code.NODEEXISTS)) {
				throw e;
			}
		}

		Thread t = new Thread(NimbusSafetyNet.getInstance());
		t.start();
		NimbusSafetyNet.getInstance().addListener(this);
	}

	@Test
	public void test() throws KeeperException, InterruptedException {
		ZooKeeper zk = Nimbus.getZooKeeper();

		notifiedcacheadded = false;
		zk.create(Nimbus.ROOT_ZNODE + "/testcache", "".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		Thread.sleep(100);
		assertTrue(notifiedcacheadded);

		notifiedcacheletadded = false;
		zk.create(Nimbus.ROOT_ZNODE + "/testcache/machine1", "".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		Thread.sleep(100);
		assertTrue(notifiedcacheletadded);

		notifiedcacheletadded = false;
		zk.create(Nimbus.ROOT_ZNODE + "/testcache/machine2", "".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		Thread.sleep(100);
		assertTrue(notifiedcacheletadded);

		notifiedcacheletremoved = false;
		zk.delete(Nimbus.ROOT_ZNODE + "/testcache/machine1", -1);
		Thread.sleep(100);
		assertTrue(notifiedcacheletremoved);

		notifiedcacheletremoved = false;
		zk.delete(Nimbus.ROOT_ZNODE + "/testcache/machine2", -1);
		Thread.sleep(100);
		assertTrue(notifiedcacheletremoved);

		notifiedcacheremoved = false;
		zk.delete(Nimbus.ROOT_ZNODE + "/testcache", -1);
		Thread.sleep(100);
		assertTrue(notifiedcacheremoved);
	}

	@Test
	public void testHeartbeat() throws KeeperException, InterruptedException,
			IOException {
		ZooKeeper zk = Nimbus.getZooKeeper();

		notifiedcacheadded = false;
		zk.create(Nimbus.ROOT_ZNODE + "/testcache", "".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		Thread.sleep(100);
		assertTrue(notifiedcacheadded);

		notifiedcacheletadded = false;
		zk.create(Nimbus.ROOT_ZNODE + "/testcache/machine1", "".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		Thread.sleep(100);
		assertTrue(notifiedcacheletadded);

		long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) <= NimbusConf.getConf()
				.getSafetyNetTimeout() * 2) {
			zk.setData(Nimbus.ROOT_ZNODE + "/testcache/machine1",
					"".getBytes(), -1);
			Thread.sleep(100);
		}

		Thread
				.sleep((long) ((float) NimbusConf.getConf()
						.getSafetyNetTimeout() * 1.5f));
		assertTrue(notifiedcachestale);
	}

	@After
	public void cleanup() throws IOException, KeeperException,
			InterruptedException {
	}

	@Override
	public void onCacheAdded(String name) {
		System.out.println("Notified cache added " + name);
		notifiedcacheadded = true;
	}

	@Override
	public void onCacheRemoved(String name) {
		System.out.println("Notified cache removed " + name);
		notifiedcacheremoved = true;
	}

	@Override
	public void onCacheletAdded(String cacheName, String cacheletName) {
		System.out.println("Notified cachelet added " + cacheletName
				+ " on cache " + cacheName);
		notifiedcacheletadded = true;
	}

	@Override
	public void onCacheletRemoved(String cacheName, String cacheletName) {
		System.out.println("Notified cachelet removed " + cacheletName
				+ " on cache " + cacheName);
		notifiedcacheletremoved = true;
	}

	@Override
	public void onCacheletStale(String cacheName, String cacheletName) {
		System.out.println("Notified cachelet stale " + cacheletName
				+ " on cache " + cacheName);
		notifiedcachestale = true;
	}
}
