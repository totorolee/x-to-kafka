/**
 * Project Name:x-to-kafka
 * File Name:LogSourceConverter.java
 * Package Name:com.renren.flume.sink.log
 * Date:2013-12-2下午2:46:34
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.producer.ProducerData;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;

/**
 * ClassName:LogSourceConverter <br/>
 * Date: 2013-12-2 下午2:46:34 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 * @since JDK 1.6
 * @see
 */
public class LogSourceConverter implements SourceConverter {

	private static final String LIST_SEPARATOR = ",";

	private static final String MAP_SEPARATOR = ":";

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.renren.flume.sink.converter.SourceConverter#convert(org.apache.flume
	 * .Event, org.apache.flume.Context)
	 */
	@Override
	public ProducerData<String, String> convert(String topic, Event event,
			Context context) throws CoverterException {
		final String attributeList = context.getString(
				"kafka.log.attribute.list", "");
		final String attributeDelimiter = context.getString(
				"kafka.log.attribute.delimiter", " ");
		final String partitionKey = context.getString(
				"kafka.log.partition.attribute", "");
		String message = new String(event.getBody());
		if (StringUtils.isBlank(attributeList))
			return new ProducerData<String, String>(topic, message);
		try {
			Map<Integer, String> attributeMap = mapColumn2Attr(attributeList);
			JSONObject data = new JSONObject();
			int index = 0;
			while (index < attributeMap.size() - 1) {
				data.put(
						attributeMap.get(index),
						message.substring(0,
								message.indexOf(attributeDelimiter)));
				message = message.substring(
						message.indexOf(attributeDelimiter) + 1,
						message.length());
				index++;
			}
			data.put(attributeMap.get(index), message);
			if (StringUtils.isBlank(partitionKey)
					|| !attributeMap.containsValue(partitionKey))
				return new ProducerData<String, String>(topic, data.toString());
			else {
				List<String> datas = new ArrayList<String>();
				datas.add(data.toString());
				return new ProducerData<String, String>(topic,
						String.valueOf(data.get(partitionKey)), datas);
			}
		} catch (Exception e) {
			throw new CoverterException(e.getMessage(), e);
		}

	}

	/**
	 * 
	 * @param attributeList
	 * @return
	 */
	private Map<Integer, String> mapColumn2Attr(String attributeList) {
		final Map<Integer, String> propMap = new HashMap<Integer, String>();
		if (attributeList.indexOf(LIST_SEPARATOR) > -1) {
			final String[] props = StringUtils.split(attributeList,
					LIST_SEPARATOR);
			for (final String prop : props) {
				final String[] kv = StringUtils.split(prop, MAP_SEPARATOR);
				propMap.put(NumberUtils.toInt(kv[0].trim()), kv[1].trim());
			}
			return propMap;
		} else {
			final String[] kv = StringUtils.split(attributeList, MAP_SEPARATOR);
			return ImmutableMap.of(NumberUtils.toInt(kv[0].trim()),
					kv[1].trim());
		}
	}
}
