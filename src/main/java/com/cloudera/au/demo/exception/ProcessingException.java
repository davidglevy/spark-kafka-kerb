package com.cloudera.au.demo.exception;

public class ProcessingException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProcessingException(String message) {
		super(message);
	}

}
