package io.jp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

@SuppressWarnings("rawtypes")
public class ConsumerVerticle extends Verticle {

	private static long NOT_RUNNING = -1;

	private long periodicTimerId = NOT_RUNNING;

	private static AtomicLong CNT = new AtomicLong();

	private class ProducerHandler implements Handler<Long> {

		@Override
		public void handle(Long event) {
			JsonObject payload = new JsonObject().putString("key", Long.toString(CNT.getAndIncrement()))
					.putString("val", "foobar");
			vertx.eventBus().send(EventBus.HBASE_PUT, payload);
			container.logger().info("Sent " + payload);
		}

	}

	private void startConsuming() {
		if (periodicTimerId == NOT_RUNNING) {
			periodicTimerId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1), new ProducerHandler());
		}
	}

	private void stopConsuming() {
		if (vertx.cancelTimer(periodicTimerId)) {
			periodicTimerId = NOT_RUNNING;
		}
	}

	@Override
	public void start() {
		startConsuming();

		vertx.eventBus().registerHandler(EventBus.CONSUMER_CONTROL, new Handler<Message<Boolean>>() {

			public void handle(Message<Boolean> event) {
				container.logger().info("Received control info: " + event.body());
				if (event.body()) {
					startConsuming();
				} else {
					stopConsuming();
				}
			}

		});
		container.logger().info(getClass().getName() + " started");
	}

}
