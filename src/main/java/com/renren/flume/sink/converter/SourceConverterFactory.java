/**
 * Project Name:x-to-kafka
 * File Name:SourceConverterFactory.java
 * Package Name:com.renren.flume.sink.converter
 * Date:2013-12-2下午4:12:32
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink.converter;

/**
 * ClassName:SourceConverterFactory <br/>
 * Date: 2013-12-2 下午4:12:32 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 */
public class SourceConverterFactory {

	public static SourceConverter getConverter(String type) {
		return SourceConverterType.getSourceConverterByType(type);
	}
}
