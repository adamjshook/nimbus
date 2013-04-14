package nimbus.usermentions;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class UserMentions {

	public static final String USER_MENTIONS_CLASS_NAME = "user.mentions.class.name";

	public static void setCacheName(Configuration conf, String cacheName) {
		conf.set(USER_MENTIONS_CLASS_NAME, cacheName);
	}

	public static String getCacheName(Configuration conf) {
		return conf.get(USER_MENTIONS_CLASS_NAME);
	}

	public static void setMentionsClass(Configuration conf,
			Class<? extends UserMentions> clazz) {
		conf.set("user.mentions.class", clazz.getCanonicalName());
	}

	public static UserMentions get(Configuration conf)
			throws ClassNotFoundException, IOException {

		String classname = conf.get("user.mentions.class");
		if (classname != null) {
			UserMentions clazz = (UserMentions) ReflectionUtils.newInstance(
					Class.forName(classname), conf);
			clazz.initialize(conf);
			return clazz;
		} else {
			return null;
		}
	}

	public abstract void initialize(Configuration conf) throws IOException;

	public abstract boolean areConnected(String userA, String userB)
			throws IOException;

	public abstract Iterator<String> getConnections(String userA)
			throws IOException;

	public abstract void close() throws IOException;

}
