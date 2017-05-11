package com.cloudera.au.demo.callback;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.cloudera.au.demo.SparkStreamingTemplate;

/**
 * Interface to capture the serial processing requirement for distributed systems.
 * 
 * Implementations are designed to be called by {@link SparkStreamingTemplate}.
 * 
 * @author davidlevy
 *
 * @param <K> Type of the {@link ConsumerRecord} key
 * @param <V> Type of the {@link ConsumerRecord} value
 */
public interface SerialProcessingCallback<K,V> extends Serializable {

	/** 
	 * Allows a callback to specify a list of options required.
	 */
	public List<Option> createOptions();
	
	/**
	 * Process the options generated.
	 * 
	 * @param commandLine
	 */
	public void processOptions(CommandLine commandLine);
	
	/**
	 * Process the given consumer record.
	 * 
	 * Exceptions generated from here will be captured by the {@link SparkStreamingTemplate} and will generate
	 * a message to the error topic as well as an error log to the enterprise application logging topic.
	 * 
	 * @param record
	 */
	public void process(ConsumerRecord<K, V> record, K key, V value);
	
}
