package nimbus.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import nimbus.main.NimbusConf;

import org.apache.log4j.Logger;

/**
 * This class is a JNI wrapper for the C++ std::set and is used by Cachelets to
 * store the actual members of a set. While there is some minor overhead using
 * JNI, it removes the JVM memory requirements issues. <br>
 * <br>
 * Note that not all methods are supported due to interop issues. <br>
 * <br>
 * The CSet is a Singleton object, due to there being no distinction of the
 * std::set. Multiple instances of a CSet would all communicate with the same
 * std::set.
 */
public class CSet implements Set<String> {

	private static final Logger LOG = Logger.getLogger(CSet.class);
	private static CSet s_instance = null;

	static {
		LOG.info("Loading native libraries from: "
				+ System.getProperty("java.library.path"));
		System.loadLibrary("NativeNimbus");
		LOG.setLevel(NimbusConf.getConf().getLog4JLevel());
	}

	public static CSet getInstance() {
		if (s_instance == null) {
			s_instance = new CSet();
		}
		return s_instance;
	}

	/**
	 * Initializes a new instance of the CSet
	 */
	private CSet() {
		c_clear();
	}

	@Override
	public boolean add(String e) {
		return c_add(e);
	}

	private native boolean c_add(String e);

	@Override
	public boolean addAll(Collection<? extends String> c) {
		boolean retval = false;
		for (String s : c) {
			retval = add(s.toString()) ? true : retval;
		}

		return retval;
	}

	@Override
	public void clear() {
		c_clear();
	}

	private native void c_clear();

	@Override
	public boolean contains(Object o) {
		return c_contains(o.toString());
	}

	private native boolean c_contains(String o);

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!contains(o)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty() {
		return c_isEmpty();
	}

	private native boolean c_isEmpty();

	@Override
	public boolean remove(Object o) {
		return c_remove(o.toString());
	}

	private native boolean c_remove(String o);

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean retval = false;
		for (Object s : c) {
			retval = remove(s.toString()) ? true : retval;
		}

		return retval;
	}

	@Override
	public int size() {
		return c_size();
	}

	private native int c_size();

	/**
	 * <b>This method is not supported and throws a RuntimeException</b>
	 * 
	 * @throws RuntimeException
	 */
	@Override
	public Iterator<String> iterator() {
		throw new RuntimeException("Not yet implemented");
	}

	/**
	 * <b>This method is not supported and throws a RuntimeException</b>
	 * 
	 * @throws RuntimeException
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		throw new RuntimeException("Not yet implemented");
	}

	/**
	 * <b>This method is not supported and throws a RuntimeException</b>
	 * 
	 * @throws RuntimeException
	 */
	@Override
	public Object[] toArray() {
		throw new RuntimeException("Not yet implemented");
	}

	/**
	 * <b>This method is not supported and throws a RuntimeException</b>
	 * 
	 * @throws RuntimeException
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object[] toArray(Object[] a) {
		throw new RuntimeException("Not yet implemented");
	}
}
