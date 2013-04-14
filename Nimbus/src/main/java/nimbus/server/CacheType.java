package nimbus.server;

/**
 * An enumeration for which type of Cache is running.
 */
public enum CacheType {
	/**
	 * The Master Cache type is the Master service as described in more detail
	 * in {@link NimbusMaster}.
	 */
	MASTER,

	/**
	 * The Distributed Set is a Cache that evenly distributes a large data set
	 * over all Cachelets in the Cache.
	 */
	DISTRIBUTED_SET,

	/**
	 * The triple store is a Cache that distributes a large data set of Triples
	 * over all Cachelets in the Cache
	 */
	TRIPLE_STORE,
	
	MAPSET
}
