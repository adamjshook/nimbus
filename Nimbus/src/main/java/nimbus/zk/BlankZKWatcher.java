package nimbus.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class BlankZKWatcher implements Watcher {

	@Override
	public void process(WatchedEvent event) {
	}
}
