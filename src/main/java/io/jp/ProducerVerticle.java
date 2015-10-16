package io.jp;

import java.util.concurrent.TimeUnit;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.impl.ReplyFailureMessage;
import org.vertx.java.platform.Verticle;


@SuppressWarnings("rawtypes")
public class ProducerVerticle extends Verticle {

	private static long NOT_RUNNING = -1;

	private long id = NOT_RUNNING;

	private class ProducerHandler implements Handler<Long> {

		@Override
		public void handle(Long event) {
			vertx.eventBus().send(EventBus.SINK_DATA, "foobar", new Handler<Message>() {

				@Override
				public void handle(Message event) {

					if (event instanceof ReplyFailureMessage) {
						container.logger().info("Got ReplyFailureMessage -> Stop consuming messages");
						stopConsuming();
					}

				}

			});
		}

	}

	private void startConsuming() {
		id = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1), new ProducerHandler());
	}

	private void stopConsuming() {
		if (vertx.cancelTimer(id)) {
			id = NOT_RUNNING;
		}
	}

	@Override
	public void start() {
		startConsuming();

		vertx.eventBus().registerHandler(EventBus.PRODUCER, new Handler<Message<Boolean>>() {

			public void handle(Message<Boolean> event) {
				if (event.body()) {
					if (id == NOT_RUNNING) {
						container.logger().info("Sink is working again -> start consuming messages");
						startConsuming();
					}
				}
			}

		});
		container.logger().info(getClass().getName() + " started");
	}

}
