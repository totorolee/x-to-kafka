/**
 * Project Name:x-to-kafka
 * File Name:CoverterException.java
 * Package Name:com.renren.flume.sink.converter
 * Date:2013-12-2下午3:03:30
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink.converter;

/**
 * ClassName:CoverterException <br/>
 * Date: 2013-12-2 下午3:03:30 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 * @since JDK 1.6
 * @see
 */
public class CoverterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -864900289818923287L;

	private static final String EXTRA_MESSAGE = "[FBI Warning: Source Content Convert Error From x-to-kafka System]-_-!------";

	public CoverterException() {
		super();
	}

	public CoverterException(String message) {
		super(EXTRA_MESSAGE + message);
	}

	public CoverterException(Throwable cause) {
		super(EXTRA_MESSAGE + cause);
	}

	public CoverterException(String message, Throwable cause) {
		super(EXTRA_MESSAGE + message, cause);
	}

}
