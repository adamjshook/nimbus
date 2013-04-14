package nimbus.utils;

import java.util.Iterator;

public class NullIterable<T> implements Iterable<T> {

	@Override
	public Iterator<T> iterator() {
		return new NullIterator<T>();
	}
}
