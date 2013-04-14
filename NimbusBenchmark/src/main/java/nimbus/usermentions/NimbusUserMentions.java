package nimbus.usermentions;

import java.io.IOException;
import java.util.Iterator;

import nimbus.client.MapSetClient;
import nimbus.master.CacheDoesNotExistException;

import org.apache.hadoop.conf.Configuration;

public class NimbusUserMentions extends UserMentions {
	private MapSetClient client = null;

	public void initialize(Configuration conf) throws IOException {
		String name = UserMentions.getCacheName(conf);
		if (name != null) {
			try {
				client = new MapSetClient(name);
			} catch (CacheDoesNotExistException e) {
				throw new IOException(e);
			}
		} else {
			throw new IOException("Cache name is not set");
		}
	}

	public boolean areConnected(String userA, String userB) throws IOException {
		return client.contains(userA, userB);
	}

	public Iterator<String> getConnections(String userA) throws IOException {
		return client.get(userA);
	}

	@Override
	public void close() throws IOException {
		if (client != null) {
			client.disconnect();
		}
	}
}
