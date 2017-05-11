package com.cloudera.au.demo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.cloudera.au.demo.callback.SerialProcessingCallback;
import com.cloudera.au.demo.exception.InitializationException;

/**
 * Wrapping template to reduce boilerplate from Spark Streaming jobs.
 * 
 * @author davidlevy
 *
 * @param <K>
 *            the {@link ConsumerRecord} key
 * @param <V>
 *            the {@link ConsumerRecord} value
 */
public class SparkStreamingTemplate implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int MIN_WINDOW_SIZE = 500;

	private final int DEFAULT_WINDOW_SIZE = 2000;

	private static final Logger logger = Logger.getLogger(SparkStreamingTemplate.class);

	private String applicationName;

	private String brokers;

	private String topic;

	private String errorTopic;

	private String hdfsDir;

	private Class keyDeserializer = StringDeserializer.class;

	private Class valueDeserializer = StringDeserializer.class;

	private String groupId = "default";

	private int windowSize = DEFAULT_WINDOW_SIZE;

	public void validate() {

		if (StringUtils.isBlank(applicationName)) {
			throw new IllegalArgumentException("Application name can not be blank");
		}
		if (StringUtils.isBlank(topic)) {
			throw new IllegalArgumentException("Topic can not be blank");
		}
		if (StringUtils.isBlank(brokers)) {
			throw new IllegalArgumentException("Brokers can not be blank");
		}
		if (StringUtils.isBlank(groupId)) {
			throw new IllegalArgumentException("Group Id can not be blank");
		}

		if (windowSize < MIN_WINDOW_SIZE) {
			throw new IllegalArgumentException("Window size [" + windowSize + "] is below minimum [" + 500 + "]");
		}
	}

	public <K, V> void parseArguments(String[] args, SerialProcessingCallback<K, V> callback)
			throws InitializationException {

		logger.info("Parsing arguments: [\"" + StringUtils.join(args, "\",\"") + "\"]");

		Options opts = createOptions(callback);

		CommandLineParser parser = new GnuParser();

		HelpFormatter formatter = new HelpFormatter();
		// TODO Inject the wrapping class name
		formatter.printHelp("spark2-submit [spark-opts] wrapping-class", opts);

		try {
			CommandLine commandLine = parser.parse(opts, args);

			applicationName = commandLine.getOptionValue("a");

			logger.info("Application Name [" + applicationName + "]");
			topic = commandLine.getOptionValue("t");
			brokers = commandLine.getOptionValue("b");
			groupId = commandLine.getOptionValue("g");
			hdfsDir = commandLine.getOptionValue("p");
			// logger.info("HDFS base directory for files is: " + hdfsDir);
			if (commandLine.hasOption("e")) {
				errorTopic = commandLine.getOptionValue("e");
			}
			if (commandLine.hasOption("w")) {
				String windowSizeText = commandLine.getOptionValue("w");
				if (StringUtils.isBlank(windowSizeText)) {
					logger.info("No window size specified, will use [" + windowSize + "]");
				} else {
					logger.info("Received window size of [" + windowSizeText + "]");
					windowSize = Integer.parseInt(windowSizeText);
				}
			}

			callback.processOptions(commandLine);

		} catch (ParseException e) {

			logger.error("Invalid options: " + e.getMessage());
			throw new InitializationException("Unable to parse options: " + e.getMessage(), e);
		}
	}

	private <K, V> Options createOptions(SerialProcessingCallback<K, V> callback) {
		logger.info("Creating Options");
		Options opts = new Options();
		Option applicationNameOpt = new Option("a", "Application Name to submit");
		applicationNameOpt.setRequired(true);
		applicationNameOpt.setLongOpt("appName");
		applicationNameOpt.setArgs(1);
		opts.addOption(applicationNameOpt);

		Option topicOpt = new Option("t", "Topic we should connect to");
		topicOpt.setRequired(true);
		topicOpt.setArgs(1);
		topicOpt.setLongOpt("topic");
		opts.addOption(topicOpt);

		Option brokerOpt = new Option("b", "Brokers we should connect to");
		brokerOpt.setRequired(true);
		brokerOpt.setArgs(1);
		brokerOpt.setLongOpt("brokers");
		opts.addOption(brokerOpt);

		Option groupIdOpt = new Option("g", "Group id");
		groupIdOpt.setArgs(1);
		groupIdOpt.setRequired(true);
		groupIdOpt.setLongOpt("groupId");
		opts.addOption(groupIdOpt);

		Option errTopicOpt = new Option("e", "Error topic we should send failures to");
		errTopicOpt.setArgs(1);
		errTopicOpt.setRequired(false);
		errTopicOpt.setLongOpt("errorTopic");
		opts.addOption(errTopicOpt);

		Option windowSizeOpt = new Option("w",
				"Window size (milliseconds), must be greater than [" + MIN_WINDOW_SIZE + "]");
		windowSizeOpt.setArgs(1);
		windowSizeOpt.setRequired(false);
		windowSizeOpt.setLongOpt("errorTopic");
		opts.addOption(windowSizeOpt);

		Option pathOption = new Option("p", "HDFS Path to save files");
		pathOption.setArgs(1);
		pathOption.setLongOpt("path");
		pathOption.setRequired(true);
		opts.addOption(pathOption);

		List<Option> callbackOptions = callback.createOptions();
		for (Option o : callbackOptions) {
			opts.addOption(o);
		}

		return opts;

	}

	public <K, V> void doInTemplate(String[] args, SerialProcessingCallback<K, V> callback)
			throws InitializationException, InterruptedException {

		parseArguments(args, callback);
		validate();

		logger.info("Will consume from topic [" + topic + "]");

		if (StringUtils.isBlank(errorTopic)) {
			errorTopic = topic + ".error";
			logger.info("Error topic is blank - setting to [" + errorTopic + "]");
		}

		SparkConf sparkConf = new SparkConf().setAppName(applicationName);

		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(windowSize));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", keyDeserializer);
		kafkaParams.put("value.deserializer", valueDeserializer);
		// TODO Change this later to use dynamic groupId
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		// append sasl property before init Kafka client
		kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
		kafkaParams.put("sasl.kerberos.service.name", "kafka");


		Properties props = new Properties();
		props.put("path", hdfsDir);
		
		Collection<String> topics = Arrays.asList(topic);
		final JavaInputDStream<ConsumerRecord<K, V>> messages = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<K, V> Subscribe(topics, kafkaParams));


		messages.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<K, V>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<K, V>> rdd) {

				final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

				logger.info(
						"\n\n#####################\n\nHere outside the forEachPartition\n\n#############################\n\n");

				rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<K, V>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<ConsumerRecord<K, V>> consumerRecords) {

						logger.info(
								"\n\n#####################\n\nHere inside the forEachPartition\n\n#############################\n\n");

						OffsetRange o = offsetRanges[TaskContext.get().partitionId()];

						while (consumerRecords.hasNext()) {
							ConsumerRecord<K, V> record = consumerRecords.next();
							V value = record.value();
							// TODO Remove this after no longer just processing
							// strings
							logger.info(StringUtils.left("Processing " + value.toString(), 200));

							try {

								callback.process(record, record.key(), value, props);
							} catch (Exception e) {
								// If we can't process a file, we need to record
								// it as badly processed - potentially submit it
								// for re-processing later.
								logger.error("Unable to process file: " + e.getMessage(), e);
							}

						}

					}
				});
			}
		});

		jssc.start(); // Start the computation
		jssc.awaitTermination();

	}

}
