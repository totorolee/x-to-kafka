/**
 * Project Name:x-to-kafka
 * File Name:KafkaSink.java
 * Package Name:com.renren.flume.sink
 * Date:2013-11-19下午7:37:05
 * Copyright (c) 2013, tomcatzhe@gmail.com All Rights Reserved.
 *
 */
package com.renren.flume.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.renren.flume.log.LogFactory;
import com.renren.flume.sink.converter.SourceConverter;
import com.renren.flume.sink.converter.SourceConverterFactory;

/**
 * ClassName:KafkaEventSink <br/>
 * Function: Flume KafkaEvent Sink. <br/>
 * Date: 2013-11-19 下午7:37:05 <br/>
 * 
 * @author changzhe.li
 * @version 1.0.0
 */
public class KafkaEventSink extends AbstractSink implements Configurable {

	private Context context;

	private Producer<String, String> producer;

	private ExecutorService callTimeoutPool;

	private SinkCounter sinkCounter;

	private String kafkaClustersPath;

	private String kafkaTopic;

	private static final String defaultSerializerClass = "kafka.serializer.StringEncoder";

	private String serializerClass;

	private static final String defaultProducerType = "sync";

	private String producerType;

	private static final String defaultCompressionCodec = "0";

	private String compressionCodec;

	private static final String defaultPartitionerClass = "kafka.producer.DefaultPartitioner";

	private String partitionerClass;

	private static final String defaultZkReadNumRetries = "3";

	private String zkReadNumRetries;

	private static final int defaultBatchSize = 100;

	private int batchSize;

	private static final int defaultThreadPoolSize = 10;

	private int threadsPoolSize;

	private static final long defaultCallTimeout = 10000;

	private long callTimeout;

	private static final String defaultSoureType = "log";

	private String sourceType;

	private static final long defaultQueueEnqueueTimeout = -1;

	private long queueEnqueueTimeout;

	private static final long defaultQueueBufferingMax = 5000;

	private long queueBufferingMax;

	private static final int defaultQueueBufferingMaxMessages = 10000;

	private int queueBufferingMaxMessages;

	private static final int defaultBatchNumMessages = 200;

	private int batchNumMessages;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		this.context = context;
		kafkaClustersPath = Preconditions.checkNotNull(
				context.getString("kafka.clusters.path"),
				"[ kafka.clusters.path ] is required.");
		kafkaTopic = Preconditions.checkNotNull(
				context.getString("kafka.topic"),
				"[ kafka.topic ] is required.");
		serializerClass = context.getString("kafka.serializer.class",
				defaultSerializerClass);
		producerType = context.getString("kafka.producer.type",
				defaultProducerType);
		compressionCodec = context.getString("kafka.compression.codec",
				defaultCompressionCodec);
		partitionerClass = context.getString("kafka.partitioner.class",
				defaultPartitionerClass);
		zkReadNumRetries = context.getString("kafka.zk.read.num.retries",
				defaultZkReadNumRetries);
		batchSize = context.getInteger("kafka.batch.size", defaultBatchSize);
		threadsPoolSize = context.getInteger("kafka.threadsPoolSize",
				defaultThreadPoolSize);
		callTimeout = context.getLong("kafka.callTimeout", defaultCallTimeout);
		sourceType = context.getString("kafka.source.type", defaultSoureType);
		queueEnqueueTimeout = context.getLong("kafka.queue.enqueue.timeout.ms",
				defaultQueueEnqueueTimeout);
		queueBufferingMax = context.getLong("kafka.queue.buffering.max.ms",
				defaultQueueBufferingMax);
		queueBufferingMaxMessages = context.getInteger(
				"kafka.queue.buffering.max.messages",
				defaultQueueBufferingMaxMessages);
		batchNumMessages = context.getInteger("kafka.batch.num.messages",
				defaultBatchNumMessages);
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public void start() {
		String timeoutName = "kafka-" + getName() + "-call-runner-%d";
		callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
				new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
		// Initialize the connection to the KAFKA
		Properties props = new Properties();
		props.put("zk.connect", this.kafkaClustersPath);
		props.put("serializer.class", this.serializerClass);
		props.put("partitioner.class", this.partitionerClass);
		props.put("producer.type", this.producerType);
		props.put("compression.codec", this.compressionCodec);
		props.put("zk.read.num.retries", this.zkReadNumRetries);
		props.put("queue.enqueue.timeout.ms", this.queueEnqueueTimeout);
		props.put("queue.buffering.max", this.queueBufferingMax);
		props.put("queue.buffering.max.messages",
				this.queueBufferingMaxMessages);
		props.put("batch.num.messages", this.batchNumMessages);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		sinkCounter.start();
		super.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			List<ProducerData<String, String>> producerData = new ArrayList<ProducerData<String, String>>();
			int txnEventCount = 0;
			for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
				Event event = ch.take();
				if (event == null) {
					break;
				}
				// Send the Event to the external repository.
				SourceConverter sc = SourceConverterFactory
						.getConverter(sourceType);
				ProducerData<String, String> data = sc.convert(kafkaTopic,
						event, context);
				producerData.add(data);
				if (txnEventCount == batchSize - 1)
					LogFactory.LOG_INFO.info(String.format(
							"Send the msg [%s] of topic [%s] to Kafka.", data
									.getData().toString(), kafkaTopic));
			}
			if (txnEventCount == 0) {
				sinkCounter.incrementBatchEmptyCount();
			} else if (txnEventCount == batchSize) {
				sinkCounter.incrementBatchCompleteCount();
			} else {
				sinkCounter.incrementBatchUnderflowCount();
			}
			flush(producerData);// Send the data to kafka
			txn.commit();
			if (txnEventCount < 1) {
				status = Status.BACKOFF;
			} else {
				sinkCounter.addToEventDrainSuccessCount(txnEventCount);
				status = Status.READY;
			}
		} catch (Throwable t) {
			txn.rollback();
			// Log exception, handle individual exceptions as needed
			status = Status.BACKOFF;
			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public void stop() {
		// Disconnect from the KAFKA and do any
		// additional cleanup
		// shut down all our thread pools
		ExecutorService toShutdown[] = { callTimeoutPool };
		for (ExecutorService execService : toShutdown) {
			execService.shutdown();
			try {
				while (execService.isTerminated() == false) {
					execService.awaitTermination(
							Math.max(defaultCallTimeout, callTimeout),
							TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException ex) {
				LogFactory.LOG_ERROR.warn("shutdown interrupted on "
						+ execService, ex);
			}
		}
		callTimeoutPool = null;
		producer.close();
		sinkCounter.stop();
		super.stop();
	}

	/**
	 * Flush kafka producer data with timeout enforced
	 * 
	 * @param producerData
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void flush(final List<ProducerData<String, String>> producerData)
			throws IOException, InterruptedException {
		callWithTimeout(new Callable<Void>() {
			public Void call() throws Exception {
				producer.send(producerData);
				return null;
			}
		});
	}

	/**
	 * Execute the callable on a separate thread and wait for the completion for
	 * the specified amount of time in milliseconds. In case of timeout cancel
	 * the callable and throw an IOException
	 */
	private <T> T callWithTimeout(Callable<T> callable) throws IOException,
			InterruptedException {
		Future<T> future = callTimeoutPool.submit(callable);
		try {
			if (callTimeout > 0) {
				return future.get(callTimeout, TimeUnit.MILLISECONDS);
			} else {
				return future.get();
			}
		} catch (TimeoutException eT) {
			future.cancel(true);
			sinkCounter.incrementConnectionFailedCount();
			throw new IOException("Callable timed out after " + callTimeout
					+ " ms", eT);
		} catch (ExecutionException e1) {
			sinkCounter.incrementConnectionFailedCount();
			Throwable cause = e1.getCause();
			if (cause instanceof IOException) {
				throw (IOException) cause;
			} else if (cause instanceof InterruptedException) {
				throw (InterruptedException) cause;
			} else if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else if (cause instanceof Error) {
				throw (Error) cause;
			} else {
				throw new RuntimeException(e1);
			}
		} catch (CancellationException ce) {
			throw new InterruptedException(
					"Blocked callable interrupted by rotation event");
		} catch (InterruptedException ex) {
			LogFactory.LOG_ERROR.warn(
					"Unexpected Exception " + ex.getMessage(), ex);
			throw ex;
		}
	}

	@Override
	public String toString() {
		return "{ Sink type:" + getClass().getSimpleName() + ", name:"
				+ getName() + " }";
	}
}
