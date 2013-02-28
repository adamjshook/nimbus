package nimbus.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

public class WriteAheadFile {

	private static final Logger LOG = Logger.getLogger(WriteAheadFile.class);
	private File file = null;
	private OutputStream out = null;
	private boolean newFile = false;

	public WriteAheadFile() throws IOException {
		// TODO
		// this.file = new
		// File(NimbusConf.getConf().getWriteAheadFile(NimbusConf.getConf().getHostname()));
		newFile = !file.exists();
	}

	public void prepareForWrite() throws IOException {

		if (!newFile) {
			out = new FileOutputStream(file, true);
		} else {
			File parentDirs = new File(file.getParent());
			LOG.info("Creating parent dirs " + parentDirs);
			parentDirs.mkdirs();

			out = new FileOutputStream(file, false);
		}
	}

	public void writeArgs(byte[][] args, int numargs) throws IOException {
		out.write('*');
		out.write(numargs);
		for (int i = 0; i < numargs; ++i) {
			out.write('$');
			out.write(args[i].length);
			out.write(args[i]);
		}
	}

	public void write(byte[] bytes) throws IOException {
		if (out != null) {
			out.write(bytes);
		}
	}

	public void flush() throws IOException {
		if (out != null) {
			out.flush();
		}
	}

	public void close() throws IOException {
		if (out != null) {
			out.close();
		}
	}

	public void delete() throws IOException {
		if (file.exists()) {
			file.delete();
		}
	}

	public File getFile() {
		return file;
	}

	public boolean isNewFile() {
		return newFile;
	}
}
