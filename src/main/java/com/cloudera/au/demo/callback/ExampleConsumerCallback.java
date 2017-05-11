package com.cloudera.au.demo.callback;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import com.cloudera.au.demo.exception.ProcessingException;
import com.cloudera.au.demo.util.HdfsUtil;

public class ExampleConsumerCallback implements
		SerialProcessingCallback<String, String> {

	private static final Logger logger = Logger
			.getLogger(ExampleConsumerCallback.class);

	private static final long serialVersionUID = 1L;

	@Override
	public void process(ConsumerRecord<String, String> record, String key,
			String value, Properties props) throws ProcessingException {

		logger.info("Processing key [" + key + "]");
		
		HdfsUtil hdfsUtil = new HdfsUtil();
		hdfsUtil.init();
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String pathPrefix = formatter.format(new Date());
			String uuid = UUID.randomUUID().toString();
			String fileName = props.getProperty("path") + pathPrefix + "/" + uuid + ".data";
			
			logger.info("Will store incoming file here: [" + fileName + "]");
			
			hdfsUtil.writeHdfs(fileName, value);

		} catch (Exception e) {
			throw new ProcessingException("Unable process incoming: "
					+ e.getMessage(), e);
		}

	}

	@Override
	public List<Option> createOptions() {
		List<Option> options = new ArrayList<>();
		return options;
	}

	/**
	 * Use the HDFS base directory declared by the parent template as our base
	 * path.
	 */
	@Override
	public void processOptions(CommandLine commandLine) {

	}

}
