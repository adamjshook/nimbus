package nimbus.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;

import nimbus.master.CacheDoesNotExistException;
import nimbus.master.NimbusMaster;
import nimbus.server.DynamicSetCacheletWorker;
import nimbus.utils.BytesUtil;

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

		super.write(DynamicSetCacheletWorker.ADD_CMD, element);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		String response = BytesUtil.toString(super.in.readArg());

		in.verifyEndOfMessage();

		if (response.equals("true")) {
			return true;
		} else if (response.equals("false")) {
			return false;
		} else {
			throw new IOException("Did not receive a true or false response.");
		}
	}

	public void addAll(Collection<? extends String> value) throws IOException {
		super.write(DynamicSetCacheletWorker.ADD_ALL_CMD, value);
	}

	public void clear() throws IOException {
		super.write(DynamicSetCacheletWorker.CLEAR_CMD);
	}

	public boolean contains(String element) throws IOException,
			CacheletNotConnectedException {
		super.write(DynamicSetCacheletWorker.CONTAINS_CMD, element);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		String response = BytesUtil.toString(super.in.readArg());

		in.verifyEndOfMessage();

		if (response.equals("true")) {
			return true;
		} else if (response.equals("false")) {
			return false;
		} else {
			throw new IOException("Did not receive a true or false response.");
		}
	}

	public boolean isEmpty() throws IOException {
		super.write(DynamicSetCacheletWorker.ISEMPTY_CMD);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		String response = BytesUtil.toString(super.in.readArg());

		in.verifyEndOfMessage();

		if (response.equals("true")) {
			return true;
		} else if (response.equals("false")) {
			return false;
		} else {
			throw new IOException("Did not receive a true or false response.");
		}
	}

	public boolean remove(String element) throws IOException {
		super.write(DynamicSetCacheletWorker.REMOVE_CMD, element);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		String response = BytesUtil.toString(super.in.readArg());

		in.verifyEndOfMessage();

		if (response.equals("true")) {
			return true;
		} else if (response.equals("false")) {
			return false;
		} else {
			throw new IOException("Did not receive a true or false response.");
		}
	}

	public boolean retainAll(Collection<? extends String> c) throws IOException {
		super.write(DynamicSetCacheletWorker.RETAIN_ALL_CMD, c);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		String response = BytesUtil.toString(super.in.readArg());

		in.verifyEndOfMessage();

		if (response.equals("true")) {
			return true;
		} else if (response.equals("false")) {
			return false;
		} else {

			throw new IOException("Did not receive a true or false response.");
		}
	}

	public int size() throws IOException {
		super.write(DynamicSetCacheletWorker.SIZE_CMD);

		if (super.in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
			throw new IOException("Did not receive ACK_CMD");
		}

		super.in.readNumArgs();

		int retval = Integer.valueOf(BytesUtil.toString(super.in.readArg()));

		in.verifyEndOfMessage();
		return retval;
	}

	public class DynamicSetCacheletIterator implements Iterator<String> {

		private long numEntries = 0, entriesRead = 0;
		private final Logger LOG = Logger
				.getLogger(DynamicSetCacheletIterator.class);

		public DynamicSetCacheletIterator() {
			try {
				write(DynamicSetCacheletWorker.GET_CMD);
				if (in.readCmd() != DynamicSetCacheletWorker.ACK_CMD) {
					throw new IOException("Did not receive ACK_CMD");
				}

				numEntries = in.readNumArgs();
				LOG.info("Need to read " + numEntries);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean hasNext() {
			if (entriesRead == numEntries) {
				try {
					in.verifyEndOfMessage();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			return entriesRead < numEntries;
		}

		@Override
		public String next() {
			++entriesRead;
			try {
				return BytesUtil.toString(in.readArg());
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
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