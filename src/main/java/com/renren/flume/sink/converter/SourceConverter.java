/**
 * Project Name:x-to-kafka
 * File Name:SourceConverter.java
 * Package Name:com.renren.flume.sink.converter
 * Date:2013-12-2下午3:02:18
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink.converter;

import kafka.javaapi.producer.ProducerData;

import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * ClassName:SourceConverter <br/>
 * Date: 2013-12-2 下午3:02:18 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 */
public interface SourceConverter {

	public ProducerData<String, String> convert(String topic, Event event,
			Context context) throws CoverterException;
}
