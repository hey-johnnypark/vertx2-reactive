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
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class JmsProducerVerticle extends Verticle {

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

			vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), new Handler<Long>() {

				@Override
				public void handle(Long event) {
					try {
						TextMessage message = session.createTextMessage();
						JsonObject jsonObject = new JsonObject();
						jsonObject.putString("key", String.format("%10d", CNT.getAndIncrement()));
						jsonObject.putString("ts", Long.toString(System.currentTimeMillis()));
						message.setText(jsonObject.toString());
						producer.send(message);
						container.logger().info("Sent " + message + " to JMS");
					} catch (JMSException e) {
						container.logger().error(e);
					}
				}
			});

		} catch (JMSException e) {
			startedResult.setFailure(e);
		}
		container.logger().info("Started");
		startedResult.setResult(null);
	}

}
