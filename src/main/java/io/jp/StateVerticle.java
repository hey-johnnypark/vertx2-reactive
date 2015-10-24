package io.jp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.platform.Verticle;

import io.jp.message.State;

public class StateVerticle extends Verticle {

	private Map<Integer, State> states = new HashMap<>();

	@Override
	public void start() {
		registerStateBus();
		initPeriodicalStateCheck();
	}

	private void initPeriodicalStateCheck() {
		vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), new Handler<Long>() {

			@Override
			public void handle(Long event) {
				container.logger().info("States are " + states);
				checkStates();
			}

		});
	}

	private void checkStates() {
		if (states.values().contains(State.ONLINE)) {
			vertx.eventBus().send(EventBus.CONSUMER_CONTROL, true);
			container.logger().info("At least one online worker -> no action");
		} else {
			container.logger().info("All workers down -> Stop consumer");
			vertx.eventBus().send(EventBus.CONSUMER_CONTROL, false);
		}
	}

	private void registerStateBus() {

		vertx.eventBus().registerHandler("state", new Handler<JsonObjectMessage>() {

			@Override
			public void handle(JsonObjectMessage event) {
				container.logger().info("Received StateMessage: " + event.body());
				states.put(event.body().getInteger("id"), State.byName(event.body().getString("state")));
				checkStates();
			}

		});

	}

}
