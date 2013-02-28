package nimbus.nativestructs;

import java.util.HashMap;
import java.util.Map;

import nimbus.nativestructs.CMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CMapTest {

	private CMap map = null;

	@Before
	public void setup() {
		map = CMap.getInstance();
	}

	@Test
	public void testContainsKey() {
		Assert.assertFalse(map.containsKey("TEST"));
		map.put("TEST", "VALUE");
		Assert.assertTrue(map.containsKey("TEST"));
	}

	@Test
	public void testContainsValue() {
		Assert.assertFalse(map.containsValue("VALUE"));
		map.put("TEST", "VALUE");
		Assert.assertTrue(map.containsValue("VALUE"));
	}

	@Test
	public void testPut() {
		Assert.assertNull(map.put("TEST", "VALUE"));
		Assert.assertEquals("VALUE", map.put("TEST", "UPDATED_VALUE"));
	}
	
	@Test
	public void testPutAll() {
		Assert.assertFalse(map.containsKey("TEST"));
		Assert.assertFalse(map.containsKey("TEST2"));
		
		Map<String, String> values = new HashMap<String, String>();		
		values.put("TEST", "VALUE");
		values.put("TEST2", "VALUE2");		
		map.putAll(values);

		Assert.assertEquals("VALUE", map.get("TEST"));
		Assert.assertEquals("VALUE2", map.get("TEST2"));		
	}

	@Test
	public void testRemove() {
		Assert.assertFalse(map.containsKey("TEST"));
		map.put("TEST", "VALUE");
		Assert.assertEquals("VALUE", map.get("TEST"));
		Assert.assertEquals("VALUE", map.remove("TEST"));
		Assert.assertFalse(map.containsKey("TEST"));
	}

	@Test
	public void testGet() {
		Assert.assertFalse(map.containsKey("TEST"));
		map.put("TEST", "VALUE");
		Assert.assertTrue(map.containsKey("TEST"));
		Assert.assertEquals("VALUE", map.get("TEST"));
	}

	@Test
	public void testSize() {
		Assert.assertEquals(0, map.size());
		map.put("TEST", "VALUE");
		Assert.assertEquals(1, map.size());
		map.put("TEST", "UPDATED_VALUE");
		Assert.assertEquals(1, map.size());
	}

	@Test
	public void testClear() {
		Assert.assertEquals(0, map.size());
		map.put("TEST", "VALUE");
		Assert.assertEquals(1, map.size());
		map.clear();
		Assert.assertEquals(0, map.size());
	}

	@Test
	public void testIsEmpty() {
		Assert.assertTrue(map.isEmpty());
		map.put("TEST", "VALUE");
		Assert.assertFalse(map.isEmpty());
	}

	@After
	public void cleanup() {
		map.clear();
	}
}
