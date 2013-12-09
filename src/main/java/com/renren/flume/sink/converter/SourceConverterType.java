/**
 * Project Name:x-to-kafka
 * File Name:SourceConverterType.java
 * Package Name:com.renren.flume.sink.converter
 * Date:2013-12-2下午4:18:18
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink.converter;

/**
 * ClassName:SourceConverterType <br/>
 * Date: 2013-12-2 下午4:18:18 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 */
public enum SourceConverterType {

	SOURCE_TYPE("log", new LogSourceConverter());

	private String type;

	private SourceConverter sourceConverter;

	SourceConverterType(String type, SourceConverter sourceConverter) {
		this.type = type;
		this.sourceConverter = sourceConverter;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public SourceConverter getSourceConverter() {
		return sourceConverter;
	}

	public void setSourceConverter(SourceConverter sourceConverter) {
		this.sourceConverter = sourceConverter;
	}

	public static SourceConverter getSourceConverterByType(String type) {
		for (final SourceConverterType item : values()) {
			if (item.getType().equalsIgnoreCase(type)) {
				return item.getSourceConverter();
			}
		}
		return null;
	}
}
