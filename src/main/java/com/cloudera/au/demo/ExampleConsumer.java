package com.cloudera.au.demo;

import java.io.Serializable;

import com.cloudera.au.demo.callback.ExampleConsumerCallback;

public class ExampleConsumer implements Serializable {

	private static final long serialVersionUID = 1L;

	private static ExampleConsumerCallback callback = new ExampleConsumerCallback();

	private static SparkStreamingTemplate template = new SparkStreamingTemplate();

	public static final void main(String[] args) throws Exception {
		template.doInTemplate(args, callback);
	}
	
}
