package nimbus.mapreduce.lib.output;

import java.io.IOException;

import nimbus.client.MapSetClient;
import nimbus.client.MasterClient;
import nimbus.master.CacheDoesNotExistException;
import nimbus.master.CacheExistsException;
import nimbus.server.CacheType;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class NimbusMapSetOutputFormat extends OutputFormat<Text, Text> {

	public static String NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME = "nimbus.mapset.output.format.cache.name";
	public static String NIMBUS_MAPSET_OUTPUT_FORMAT_CREATE_CACHE = "nimbus.mapset.output.format.create.cache";

	public static void setCacheName(Job job, String name) {
		job.getConfiguration()
				.set(NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME, name);
	}

	public static void setCreateCacheIfNotExist(Job job, boolean val) {
		job.getConfiguration().setBoolean(
				NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME, val);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {

		String cacheName = context.getConfiguration().get(
				NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME);
		if (cacheName == null) {
			throw new IOException(NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME
					+ " is not set");
		}

		MasterClient client = new MasterClient();
		if (context.getConfiguration().getBoolean(
				NIMBUS_MAPSET_OUTPUT_FORMAT_CREATE_CACHE, true)) {
			try {
				client.createCache(cacheName, CacheType.MAPSET);
			} catch (CacheExistsException e) {
				// ignore
			}
			client.disconnect();
		} else if (!client.exists(cacheName)) {
			client.disconnect();
			throw new IOException(NIMBUS_MAPSET_OUTPUT_FORMAT_CREATE_CACHE
					+ " is false and cache does not exist");
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new NullOutputFormat<Text, Text>().getOutputCommitter(context);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new NimbusMapSetRecordWriter(context.getConfiguration().get(
				NIMBUS_MAPSET_OUTPUT_FORMAT_CACHE_NAME));
	}

	public static class NimbusMapSetRecordWriter extends
			RecordWriter<Text, Text> {

		private MapSetClient client = null;

		public NimbusMapSetRecordWriter(String cacheName) throws IOException {
			try {
				client = new MapSetClient(cacheName);
			} catch (CacheDoesNotExistException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			client.disconnect();
		}

		@Override
		public void write(Text key, Text value) throws IOException,
				InterruptedException {
			client.add(key.toString(), value.toString());
		}
	}
}