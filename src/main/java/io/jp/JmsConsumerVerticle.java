package io.jp;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class JmsConsumerVerticle extends Verticle {

	private Connection connection;

	@Override
	public void start(Future<Void> startedResult) {
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			connection = connectionFactory.createConnection();

			Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			Destination destination = session.createQueue("foobar");
			MessageConsumer consumer = session.createConsumer(destination);
			consumer.setMessageListener(new MessageListener() {

				@Override
				public void onMessage(Message message) {
					try {
						if (message instanceof TextMessage) {
							JsonObject payload = new JsonObject(((TextMessage) message).getText());
							vertx.eventBus().sendWithTimeout(EventBus.HBASE_PUT, payload, 1000L, ackHandler(message));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

			connection.start();
		} catch (Exception e) {
			startedResult.setFailure(e);
		}
		container.logger().info("Started");
		startedResult.setResult(null);
	}

	@Override
	public void stop() {
		try {
			connection.close();
		} catch (Exception e) {
			container.logger().error(e);
		}
	}

	private AcknowledgeReplyHandler ackHandler(Message message) {
		return new AcknowledgeReplyHandler(message);
	}

}
