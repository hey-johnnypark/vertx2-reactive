package io.jp;

import java.util.concurrent.atomic.AtomicInteger;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

import io.jp.message.State;
import io.jp.message.StateMessage;

public class DummySinkVerticle extends Verticle {

	private static final AtomicInteger INSTANCE_ID = new AtomicInteger(0);

	private int instanceId = INSTANCE_ID.getAndIncrement();

	private boolean works = true;

	private class SinkDataHandler implements Handler<Message<String>> {

		@Override
		public void handle(Message<String> event) {
			try {
				sink(event.body());
			} catch (RuntimeException e) {
				container.logger().info("Exception: ", e);
				vertx.eventBus().unregisterHandler(EventBus.SINK_DATA, this);
				sendState(State.OFFLINE);
			}

		}
	}

	@Override
	public void start() {

		container.logger().info("Started");

		sendState(State.ONLINE);

		final SinkDataHandler handler = new SinkDataHandler();
		vertx.eventBus().registerHandler(EventBus.SINK_DATA, handler);

		vertx.eventBus().registerHandler(EventBus.SINK_CONTROL, new Handler<Message<Boolean>>() {

			@Override
			public void handle(Message<Boolean> event) {
				works = event.body();
				if (works) {
					vertx.eventBus().registerHandler(EventBus.SINK_DATA, handler);
					sendState(State.ONLINE);
				}
			}

		});

	}

	private void sendState(State state) {
		vertx.eventBus().send("state", new StateMessage().withId(instanceId).withState(state));
	}

	private void sink(String message) {
		if (works) {
			container.logger().info(Thread.currentThread() + " -> Sent message to sink: " + message);
		} else {
			throw new RuntimeException("Cannot sent message I am not working correctly");
		}
	}

}
