package io.jp.verticles;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.EventBus;
import io.jp.handlers.AcknowledgeReplyHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class JmsConsumerVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(JmsConsumerVerticle.class);

	private Connection connection;

	@Override
	public void start(Future<Void> startedResult) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connection = connectionFactory.createConnection();

			Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			Destination destination = session.createQueue("foobar");
			MessageConsumer consumer = session.createConsumer(destination);

			consumer.setMessageListener(jmsMessage -> {
				try {
					if (jmsMessage instanceof TextMessage) {
						JsonObject payload = new JsonObject(((TextMessage) jmsMessage).getText());
						vertx.eventBus().send(EventBus.HBASE_PUT, payload, ackHandler(jmsMessage));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});

			connection.start();
		} catch (Exception e) {
			startedResult.fail(e);
		}
		LOG.info("Started");
		startedResult.complete();
	}

	@Override
	public void stop() {
		try {
			connection.close();
		} catch (Exception e) {
			LOG.error("E", e);
		}
	}

	private AcknowledgeReplyHandler ackHandler(Message message) {
		return new AcknowledgeReplyHandler(message);
	}

}