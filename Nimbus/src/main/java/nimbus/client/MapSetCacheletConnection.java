package nimbus.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.MapSetCacheletWorker;

public class MapSetCacheletConnection extends BaseNimbusClient implements
		Iterable<Entry<String, String>> {

	private static final Logger LOG = Logger
			.getLogger(MapSetCacheletConnection.class);

	/**
	 * Connects to the given host. Automatically gets the port from the Master
	 * based on the given Cache name.
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
	public MapSetCacheletConnection(String cacheName, String cacheletName)
			throws CacheDoesNotExistException, IOException {
		super(cacheletName, NimbusMaster.getInstance().getCachePort(cacheName));
		connect();
		LOG.info("Connected to " + cacheletName);
	}

	public void add(String key, String value) throws IOException {
		writeLine(MapSetCacheletWorker.ADD + " " + key + " " + value);
	}

	public void remove(String key) throws IOException {
		writeLine(MapSetCacheletWorker.REMOVE_KEY + " " + key);
	}

	public void remove(String key, String value) throws IOException {
		writeLine(MapSetCacheletWorker.REMOVE_KEY_VALUE + " " + key + " "
				+ value);
	}

	public boolean contains(String key) throws IOException {
		writeLine(MapSetCacheletWorker.CONTAINS_KEY + " " + key);
		return Boolean.parseBoolean(super.readLine());
	}

	public boolean contains(String key, String value) throws IOException {
		writeLine(MapSetCacheletWorker.CONTAINS_KEY_VALUE + " " + key + " "
				+ value);
		return Boolean.parseBoolean(super.readLine());
	}

	public boolean isEmpty() throws IOException {
		writeLine("" + MapSetCacheletWorker.ISEMPTY);
		return Boolean.parseBoolean(super.readLine());
	}

	public void clear() throws IOException {
		writeLine("" + MapSetCacheletWorker.CLEAR);
	}

	public int size() throws IOException {
		writeLine("" + MapSetCacheletWorker.SIZE);
		return Integer.parseInt(super.readLine());
	}

	public Iterator<String> get(String key) throws IOException {
		return new SetCacheletIterator(this, key);
	}

	@Override
	public Iterator<Entry<String, String>> iterator() {
		try {
			return new MapSetCacheletIterator(this);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public class SetCacheletIterator implements Iterator<String> {

		private int size, read = 0;

		public SetCacheletIterator(MapSetCacheletConnection client, String key)
				throws IOException {
			client.writeLine(MapSetCacheletWorker.GET + " " + key);
			size = Integer.parseInt(readLine());
		}

		public float getProgress() {
			return (float) read / (float) size;
		}

		@Override
		public boolean hasNext() {
			return read < size;
		}

		@Override
		public String next() {

			try {
				if (read >= size) {
					return null;
				}

				++read;
				return readLine();
			} catch (IOException e) {
				e.printStackTrace();
				read = size;
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"MapSetCacheletIterator::remove is not supported");
		}
	}

	public class MapSetCacheletIterator implements
			Iterator<Entry<String, String>> {

		private int size, read = 0;
		private String[] tokens = null;

		public MapSetCacheletIterator(MapSetCacheletConnection client)
				throws IOException {
			client.writeLine("" + MapSetCacheletWorker.GET_ALL);
			size = Integer.parseInt(readLine());
		}

		public float getProgress() {
			return (float) read / (float) size;
		}

		@Override
		public boolean hasNext() {
			return read < size;
		}

		@Override
		public Entry<String, String> next() {

			try {
				if (read >= size) {
					return null;
				}

				tokens = readLine().split("\t");
				++read;
				return new MapSetEntry(tokens[0], tokens[1]);
			} catch (IOException e) {
				e.printStackTrace();
				read = size;
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"MapSetCacheletIterator::remove is not supported");
		}
	}
}