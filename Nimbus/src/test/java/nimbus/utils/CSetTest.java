package nimbus.utils;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CSetTest {

	private CSet set = null;

	@Before
	public void setup() {
		set = CSet.getInstance();
	}

	@Test
	public void testContains() {
		Assert.assertFalse(set.contains("TEST"));
		set.add("TEST");
		Assert.assertTrue(set.contains("TEST"));
	}

	@Test
	public void testContainsAll() {
		Assert.assertFalse(set.contains("TEST"));
		Assert.assertFalse(set.contains("TEST2"));

		set.add("TEST");
		set.add("TEST2");

		Set<String> values = new HashSet<String>();
		values.add("TEST");
		values.add("TEST2");
		Assert.assertTrue(set.containsAll(values));
	}

	@Test
	public void testAdd() {
		Assert.assertTrue(set.add("TEST"));
		Assert.assertTrue(set.contains("TEST"));
	}

	@Test
	public void testAddAll() {
		Assert.assertFalse(set.contains("TEST"));
		Assert.assertFalse(set.contains("TEST2"));

		Set<String> values = new HashSet<String>();
		values.add("TEST");
		values.add("TEST2");

		set.addAll(values);
		Assert.assertTrue(set.contains("TEST"));
		Assert.assertTrue(set.contains("TEST2"));
		Assert.assertTrue(set.containsAll(values));
	}

	@Test
	public void testRemove() {
		Assert.assertFalse(set.contains("TEST"));
		set.add("TEST");
		Assert.assertTrue(set.contains("TEST"));
		Assert.assertTrue(set.remove("TEST"));
		Assert.assertFalse(set.contains("TEST"));
		Assert.assertFalse(set.remove("TEST"));
	}

	@Test
	public void testSize() {
		Assert.assertEquals(0, set.size());
		set.add("TEST");
		Assert.assertEquals(1, set.size());
		set.add("TEST");
		Assert.assertEquals(1, set.size());
	}

	@Test
	public void testClear() {
		Assert.assertEquals(0, set.size());
		set.add("TEST");
		Assert.assertEquals(1, set.size());
		set.clear();
		Assert.assertEquals(0, set.size());
	}

	@Test
	public void testIsEmpty() {
		Assert.assertTrue(set.isEmpty());
		set.add("TEST");
		Assert.assertFalse(set.isEmpty());
	}

	@After
	public void cleanup() {
		set.clear();
	}
}
