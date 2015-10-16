package io.jp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

public class SinkVerticle extends Verticle {

	private boolean works = true;

	@Override
	public void start() {

		vertx.eventBus().registerHandler(EventBus.SINK_DATA, new Handler<Message<String>>() {

			@Override
			public void handle(Message<String> event) {
				try {
					sink(event.body());
				} catch (RuntimeException e) {
					container.logger().info("Exception: ", e);
					event.fail(0, event.body());
				}
			}

		});

		vertx.eventBus().registerHandler(EventBus.SINK_CONTROL, new Handler<Message<Boolean>>() {

			@Override
			public void handle(Message<Boolean> event) {
				works = event.body();
				if (works) {
					vertx.eventBus().send(EventBus.CONSUMER, true);
				}
			}

		});

	}

	private void sink(String message) {
		if (works) {
			container.logger().info("Sent message to sink: " + message);
		} else {
			throw new RuntimeException("Cannot sent message I am not working correctly");
		}
	}

}
