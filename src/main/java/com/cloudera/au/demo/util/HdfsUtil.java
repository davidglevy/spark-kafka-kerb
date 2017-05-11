package com.cloudera.au.demo.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.au.demo.exception.InitializationException;

public class HdfsUtil {

	private static Logger logger = Logger.getLogger(HdfsUtil.class);

	private Configuration hdfsConf;

	public void init() throws InitializationException {

		hdfsConf = new Configuration();
		
		// TODO Move this out of here into RuntimePropertyConfig
		String defaultFs = "hdfs://toot-nn-1:8020";
		
		// Check if the defaultFs is blank - if so do not set it.
		// This may happen as part of the bootstrap process (initial load of the
		// runtime properties).
		if (StringUtils.isBlank(defaultFs)) {
			logger.warn("Default filesystem is blank so not setting");
		} else {
			logger.info("Default filesystem is: [" + defaultFs + "]");
			hdfsConf.set("fs.defaultFS", defaultFs);
		}

	}

	public void writeHdfs(String dest, String content) throws IOException {
		writeHdfs(dest, content.getBytes());
	}

	public void writeHdfs(String dest, byte[] content) throws IOException {
		logger.info("DefaultFS before write is [" + hdfsConf.get("fs.defaultFS") + "]");
		FileSystem fs = FileSystem.get(URI.create(dest), hdfsConf);
		try (OutputStream out = fs.create(new Path(dest));
				ByteArrayInputStream in = new ByteArrayInputStream(content)) {
			// Copy file from local to HDFS
			IOUtils.copy(in, out);
			out.flush();
		}
	}

	/**
	 * Good method for reading small files into memory. Not for big (>1M files).
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public byte[] readHdfs(String path) throws IOException {
		logger.info("DefaultFS before write is [" + hdfsConf.get("fs.defaultFS") + "]");
		FileSystem fs = FileSystem.get(hdfsConf);
		byte[] result = null;
		try (InputStream in = fs.open(new Path(path)); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			IOUtils.copy(in, out);
			result = out.toByteArray();
		}
		return result;

	}


}
