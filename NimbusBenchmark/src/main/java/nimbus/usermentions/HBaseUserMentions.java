package nimbus.usermentions;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUserMentions extends UserMentions {

	public static final byte[] COLUMN_FAMILY = "m".getBytes();
	private HTable table = null;

	@Override
	public void initialize(Configuration conf) throws IOException {
		String name = UserMentions.getCacheName(conf);
		if (name != null) {
			table = new HTable(HBaseConfiguration.create(conf),
					UserMentions.getCacheName(conf));
		} else {
			throw new IOException("Table name is not set");
		}
	}

	@Override
	public boolean areConnected(String userA, String userB) throws IOException {
		Get get = new Get(userA.getBytes());
		get.addColumn(COLUMN_FAMILY, userB.getBytes());
		return table.get(get).isEmpty();
	}

	@Override
	public Iterator<String> getConnections(String userA) throws IOException {
		Get get = new Get(userA.getBytes());
		get.addFamily(COLUMN_FAMILY);
		return new HBaseUserConnectionIterator(table.get(get));
	}

	@Override
	public void close() throws IOException {
		table.close();
	}

	public class HBaseUserConnectionIterator implements Iterator<String> {

		private Iterator<Entry<byte[], byte[]>> iter = null;

		public HBaseUserConnectionIterator(Result result) {
			iter = result.getNoVersionMap().get(COLUMN_FAMILY).entrySet()
					.iterator();
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public String next() {
			if (iter.hasNext()) {
				return Bytes.toString(iter.next().getValue());
			} else {
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"HBaseUserConnectionIterator::remove is unsupported");
		}
	}
}
