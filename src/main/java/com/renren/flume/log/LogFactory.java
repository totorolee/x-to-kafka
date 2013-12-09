/**
 * Project Name:x-to-kafka
 * File Name:LogFactory.java
 * Package Name:com.renren.flume.log
 * Date:2013-11-20下午3:24:10
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.log;

import org.apache.log4j.Logger;

/**
 * ClassName:LogFactory <br/>
 * Date: 2013-11-20 下午3:24:10 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 */
public interface LogFactory {

	public static final Logger LOG_INFO = Logger.getLogger("handler");// 打印正常信息

	public static final Logger LOG_ERROR = Logger.getLogger("handler_error");// 打印错误信息

}
