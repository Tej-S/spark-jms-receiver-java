package de.berschae.spark.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import de.berschae.spark.jms.MessageConsumerFactory;
import de.berschae.spark.jms.SynchronousJmsReceiver;
import de.berschae.spark.jms.SynchronousJmsReceiver.MessageConverterFunction;

/**
 * Tests of {@link SynchronousJmsReceiver}
 * 
 * @author Bernhard Schaefer
 *
 */
public class SynchronousJmsReceiverTest {
	private static final Logger log = LoggerFactory.getLogger(SynchronousJmsReceiverTest.class);
	private static final String queueName = "testQueue";

	private static MessageConsumerFactory consumerFactory;

	@BeforeClass
	public static void beforeClass() throws JMSException {
		consumerFactory = new ActiveConsumerFactory(queueName);
	}

	@AfterClass
	public static void afterClass() throws JMSException {
		consumerFactory.stopConnection();
	}

	private static class ActiveConsumerFactory extends MessageConsumerFactory {
		private static final long serialVersionUID = 1L;

		private transient ConnectionFactory connectionFactory = null;

		private final String queueName;

		public ActiveConsumerFactory(String queueName) {
			this.queueName = queueName;
		}

		@Override
		public Connection makeConnection() throws JMSException {
			if (connectionFactory == null) {
				connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
			}
			return connectionFactory.createConnection();
		}

		@Override
		public MessageConsumer makeConsumer(Session session) throws JMSException {
			return session.createConsumer(session.createQueue(queueName));
		}

	}

	@Test
	public void testReceival() throws JMSException, InterruptedException {
		Duration blockInterval = Durations.milliseconds(200);

		MessageConverterFunction<String> messageConverter = msg -> {
			try {
				return Optional.of(((TextMessage) msg).getText());
			} catch (JMSException e) {
				throw new RuntimeException(e);
			}
		};
		SynchronousJmsReceiver<String> receiver = new SynchronousJmsReceiver<String>(consumerFactory, //
				messageConverter, //
				blockInterval, //
				StorageLevel.MEMORY_ONLY());

		List<String> results = new ArrayList<>();

		Duration batchDuration = Durations.seconds(1);
		try (JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "test-app", batchDuration)) {
			JavaReceiverInputDStream<String> stream = jssc.receiverStream(receiver);
			stream.foreachRDD(rdd -> {
				List<String> msgs = rdd.collect();
				log.info("received rdd with {} partitions and {} messages", rdd.partitions().size(), msgs.size());
				results.addAll(msgs);
			});

			jssc.start();

			long millisPerMessage = 10;
			int numOfMessages = (int) (2 * batchDuration.milliseconds() / millisPerMessage);

			sendMessages(queueName, numOfMessages, millisPerMessage);
			Thread.sleep(batchDuration.milliseconds());
			log.info("\tFINISHED FIRST ROUND");
			Assertions.assertThat(results).hasSize(numOfMessages);

			results.clear();

			sendMessages(queueName, numOfMessages, millisPerMessage);
			Thread.sleep(batchDuration.milliseconds());
			log.info("\tFINISHED SECOND ROUND");
			Assertions.assertThat(results).hasSize(numOfMessages);
		}
	}

	private void sendMessages(String queueName, int numMessages, long millisPerMessage) throws JMSException, InterruptedException {
		Connection connection = consumerFactory.makeConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer messageProducer = session.createProducer(session.createQueue(queueName));

		Stopwatch stopWatch = new Stopwatch().start();
		for (int i = 0; i < numMessages; i++) {
			TextMessage textMsg = session.createTextMessage();
			textMsg.setText("message " + i);
			messageProducer.send(textMsg);
			long sleepTime = millisPerMessage - stopWatch.elapsedTime(TimeUnit.MILLISECONDS);
			if (sleepTime > 0) {
				Thread.sleep(sleepTime);
			}
			stopWatch.reset().start();
		}

		messageProducer.close();
		session.close();
	}
}
