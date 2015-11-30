package io.jp.verticles;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.EventBus;
import io.jp.message.State;
import io.jp.message.StateMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;

public class DummySinkVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(DummySinkVerticle.class);

	private static final AtomicInteger INSTANCE_ID = new AtomicInteger(0);

	private int instanceId = INSTANCE_ID.getAndIncrement();

	private boolean works = true;

	@Override
	public void start() {

		LOG.info("Started");

		sendState(State.ONLINE);

		final Handler<Message<String>> handler = event -> {
			try {
				sink(event.body());
			} catch (RuntimeException e) {
				LOG.info("Exception: ", e);
				sendState(State.OFFLINE);
			}
		};

		vertx.eventBus().consumer(EventBus.SINK_DATA, handler);

		vertx.eventBus().consumer(EventBus.SINK_CONTROL, event -> {
			works = (boolean) event.body();
			if (works) {
				vertx.eventBus().consumer(EventBus.SINK_DATA, handler);
				sendState(State.ONLINE);
			}
		});
	}

	private void sendState(State state) {
		vertx.eventBus().send("state", new StateMessage().withId(instanceId).withState(state));
	}

	private void sink(String message) {
		if (works) {
			LOG.info("{} -> Sent message to sink: {}", Thread.currentThread(), message);
		} else {
			throw new RuntimeException("Cannot sent message I am not working correctly");
		}
	}

}
