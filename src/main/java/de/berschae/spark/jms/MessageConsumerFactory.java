package de.berschae.spark.jms;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement to setup Jms consumer programmatically. Must be serializable i.e. Don't put Jms object in non transient fields.
 * 
 * @author Bernhard Schaefer
 */
public abstract class MessageConsumerFactory implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(MessageConsumerFactory.class);

	private volatile transient Connection connection = null;

	public MessageConsumer newConsumer(int acknowledgeMode) throws JMSException {
		log.info("Creating new MessageConsumer with acknowledgeMode={}", acknowledgeMode);
		stopConnection();
		connection = makeConnection();
		Session session = makeSession(acknowledgeMode);
		MessageConsumer consumer = makeConsumer(session);
		log.info("Starting new connection");
		connection.start();
		log.info("Successfully started new connection");
		return consumer;
	}

	private Session makeSession(int acknowledgeMode) throws JMSException {
		return connection.createSession(false, acknowledgeMode);
	}

	public void stopConnection() throws JMSException {
		if (connection != null) {
			try {
				log.info("Closing existing connection");
				connection.close();
			} finally {
				connection = null;
			}
		}
	}

	/**
	 * Implement to make new connection
	 *
	 * @return
	 */
	public abstract Connection makeConnection() throws JMSException;

	/**
	 * Build new consumer
	 *
	 * @param session
	 * @return
	 */
	public abstract MessageConsumer makeConsumer(Session session) throws JMSException;
}