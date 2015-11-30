package io.jp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class JmsProducerVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(JmsProducerVerticle.class);

	private static AtomicLong CNT = new AtomicLong();

	@Override
	public void start(Future<Void> startedResult) {

		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			Connection connection;
			connection = connectionFactory.createConnection();
			connection.start();
			final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("foobar");
			final MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), time -> {
				try {
					TextMessage message = session.createTextMessage();
					JsonObject jsonObject = new JsonObject();
					jsonObject.put("key", String.format("%10d", CNT.getAndIncrement()));
					jsonObject.put("ts", Long.toString(System.currentTimeMillis()));
					message.setText(jsonObject.toString());
					producer.send(message);
					LOG.info("Sent " + message + " to JMS");
				} catch (JMSException e) {
					LOG.error("E", e);
				}
			});
			startedResult.complete();
			LOG.info("Started");
		} catch (JMSException e) {
			startedResult.fail(e);
		}
	}

}
