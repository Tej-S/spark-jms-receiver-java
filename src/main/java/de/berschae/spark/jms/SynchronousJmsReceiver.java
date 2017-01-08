package de.berschae.spark.jms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StreamBlockId;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.receiver.BlockGenerator;
import org.apache.spark.streaming.receiver.BlockGeneratorListener;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import scala.collection.mutable.ArrayBuffer;

/**
 * Synchronous Reliable Receiver to use for a Jms provider that does not support an individual acknowledgment mode. Supports all Spark Streaming properties such
 * as spark.streaming.backpressure.enabled, spark.streaming.blockInterval, and spark.streaming.receiver.maxRate.
 * 
 * @author Bernhard Schaefer
 *
 * @param <T>
 */
public class SynchronousJmsReceiver<T> extends Receiver<T> {
	static final Logger log = LoggerFactory.getLogger(SynchronousJmsReceiver.class);
	private static final long serialVersionUID = 1L;

	final MessageConsumerFactory consumerFactory;
	final MessageConverterFunction<T> messageConverter;
	final long blockIntervalMillis;

	transient volatile BlockGenerator blockGenerator = null;

	private transient volatile Optional<ExecutorService> receiverThread = Optional.empty();

	/**
	 * Serializable lambda function for converting a {@link Message} to an optional type.
	 *
	 * @param <T>
	 *            the generic type to convert to
	 */
	public interface MessageConverterFunction<T> extends Function<Message, Optional<T>>, Serializable {
	}

	/**
	 * @param consumerFactory
	 *            Implementation specific factory for building MessageConsumer. Use JndiMessageConsumerFactory to setup via JNDI
	 * @param messageConverter
	 *            Function to map from Message type to T. Return None to filter out message
	 * @param sparkConf
	 *            for retrieving spark.streaming.blockInterval
	 * @param storageLevel
	 */
	public SynchronousJmsReceiver(MessageConsumerFactory consumerFactory, MessageConverterFunction<T> messageConverter, SparkConf sparkConf,
			final StorageLevel storageLevel) {
		this(consumerFactory, messageConverter, Durations.milliseconds(sparkConf.getInt("spark.streaming.blockInterval", 200)), storageLevel);
	}

	/**
	 * @param consumerFactory
	 *            Implementation specific factory for building MessageConsumer. Use JndiMessageConsumerFactory to setup via JNDI
	 * @param messageConverter
	 *            Function to map from Message type to T. Return None to filter out message
	 * @param blockInterval
	 *            equivalent to Spark Streaming property spark.streaming.blockInterval
	 * @param storageLevel
	 */
	public SynchronousJmsReceiver(MessageConsumerFactory consumerFactory, MessageConverterFunction<T> messageConverter, Duration blockInterval,
			final StorageLevel storageLevel) {
		super(storageLevel);
		this.consumerFactory = consumerFactory;
		this.messageConverter = messageConverter;
		this.blockIntervalMillis = blockInterval.milliseconds();
	}

	@Override
	public void onStart() {
		log.info("Called receiver onStart() with blockInterval={}", blockIntervalMillis);
		receiverThread = Optional.of(Executors.newSingleThreadExecutor());
		// We are just using the blockGenerator for access to rate limiter
		this.blockGenerator = supervisor().createBlockGenerator(new GeneratedBlockHandler());

		receiverThread.get().execute(new JMSReceiverThread());
	}

	@Override
	public void onStop() {
		log.info("Called receiver onStop()");

		if (receiverThread.isPresent()) {
			receiverThread.get().shutdown();

			long terminationTimeoutMillis = 1000L;

			boolean executorTerminated = false;
			try {
				executorTerminated = receiverThread.get().awaitTermination(terminationTimeoutMillis, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				log.warn(e.getLocalizedMessage(), e);
			}

			if (executorTerminated) {
				log.info("Receiver thread terminated gracefully within {} ms", terminationTimeoutMillis);
			} else {
				log.warn("Shutting down receiver thread immediately");
				receiverThread.get().shutdownNow();
			}
		}

		if (blockGenerator != null) {
			if (blockGenerator.isActive()) {
				log.info("Stopping active BlockGenerator");
				blockGenerator.stop();
			}
			blockGenerator = null;
		}
	}

	private class JMSReceiverThread implements Runnable {
		private final List<Message> buffer = new ArrayList<>();
		private final Stopwatch stopWatch = new Stopwatch().start();

		public JMSReceiverThread() {
		}

		@Override
		public void run() {
			try {
				MessageConsumer consumer = consumerFactory.newConsumer(Session.CLIENT_ACKNOWLEDGE);

				while (!isStopped()) {
					long elapsedBatchMillis = stopWatch.elapsedTime(TimeUnit.MILLISECONDS);

					if (elapsedBatchMillis >= blockIntervalMillis) {
						log.info("Calling storeBuffer() with {} messages after {}", buffer.size(), elapsedBatchMillis);
						storeBuffer();
						elapsedBatchMillis = stopWatch.elapsedTime(TimeUnit.MILLISECONDS); // get elapsed time of new batch
					}

					long receiveTimeout = blockIntervalMillis - elapsedBatchMillis;
					log.trace("Receiving message with timeout {}", receiveTimeout);
					Message message = receiveTimeout > 0 ? consumer.receive(receiveTimeout) : consumer.receiveNoWait();
					if (message != null) {
						blockGenerator.waitToPush(); // Use rate limiter
						log.trace("Adding message to buffer");
						buffer.add(message);
					}
				}

			} catch (Exception e) {
				log.error(e.getLocalizedMessage(), e);
				restart(e.getLocalizedMessage(), e);
			} finally {
				try {
					consumerFactory.stopConnection();
					log.info("Successfully stopped connection");
				} catch (JMSException e) {
					log.warn(e.getLocalizedMessage(), e);
				}
			}
			log.info("Finished run() in receiver thread");
		}

		public void storeBuffer() {
			if (!buffer.isEmpty()) {
				Iterator<T> msgs = buffer.stream() //
						.map(messageConverter) // convert Message to T
						.filter(Optional::isPresent).map(Optional::get) // filter out absent Optionals
						.iterator();
				store(msgs);

				// only acknowledges the session if there was no Exception while converting the messages and storing in Spark memory
				try {
					buffer.get(buffer.size() - 1).acknowledge();
				} catch (JMSException e) {
					log.warn("Acknowledgement failed", e);
				}

				buffer.clear();
			}
			stopWatch.reset().start();
		}
	}

	/**
	 * Dummy {@link BlockGeneratorListener} to get access to the rate limiter of {@link BlockGenerator}.
	 */
	private static class GeneratedBlockHandler implements BlockGeneratorListener {
		@Override
		public void onAddData(Object arg0, Object arg1) {
		}

		@Override
		public void onError(String arg0, Throwable arg1) {
		}

		@Override
		public void onGenerateBlock(StreamBlockId arg0) {
		}

		@Override
		public void onPushBlock(StreamBlockId arg0, ArrayBuffer<?> arg1) {
		}
	}

}