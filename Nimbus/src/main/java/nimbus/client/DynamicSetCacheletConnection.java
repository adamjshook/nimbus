package nimbus.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.DynamicSetCacheletWorker;

public class DynamicSetCacheletConnection extends BaseNimbusClient implements
		Iterable<String> {

	public DynamicSetCacheletConnection(String cacheName, String cacheletName)
			throws CacheDoesNotExistException, IOException {
		super(cacheletName, NimbusMaster.getInstance().getCachePort(cacheName));
		super.cacheName = cacheName;
		connect();
	}

	public Iterator<String> iterator() {
		return new DynamicSetCacheletIterator();
	}

	public boolean add(String element) throws IOException {
		writeLine(DynamicSetCacheletWorker.ADD + " " + element);
		return Boolean.parseBoolean(readLine());
	}

	public void addAll(Collection<? extends String> value)
			throws IOException {
		writeLine(DynamicSetCacheletWorker.ADD_ALL + " " + value.size());
		writeLines(value);
	}

	public void clear() throws IOException {
		writeLine(DynamicSetCacheletWorker.CLEAR);
	}

	public boolean contains(String element) throws IOException,
			CacheletNotConnectedException {
		writeLine(DynamicSetCacheletWorker.CONTAINS + " " + element);
		return Boolean.parseBoolean(readLine());
	}

	public boolean isEmpty() throws IOException {
		writeLine(DynamicSetCacheletWorker.ISEMPTY);
		return Boolean.parseBoolean(readLine());
	}

	public boolean remove(String element) throws IOException {
		writeLine(DynamicSetCacheletWorker.REMOVE + " " + element);
		return Boolean.parseBoolean(readLine());
	}

	public boolean retainAll(Collection<String> c) throws IOException {
		writeLine(DynamicSetCacheletWorker.RETAIN_ALL + " " + c.size());
		writeLines(c);
		return Boolean.parseBoolean(readLine());
	}

	public int size() throws IOException {
		writeLine(DynamicSetCacheletWorker.SIZE);
		return Integer.parseInt(readLine());
	}

	public class DynamicSetCacheletIterator implements Iterator<String> {

		private int numEntries = 0;
		private int entriesRead = 0;

		public DynamicSetCacheletIterator() {
			try {
				writeLine(Integer.toString(DynamicSetCacheletWorker.GET));
				numEntries = Integer.parseInt(readLine());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean hasNext() {
			return entriesRead < numEntries;
		}

		@Override
		public String next() {
			++entriesRead;
			try {
				return readLine();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public void remove() {
			throw new RuntimeException(
					"NimbusSetCacheletIterator::remove is unsupported");
		}

		public float getProgress() {
			return (float) entriesRead / (float) numEntries;
		}
	}
}