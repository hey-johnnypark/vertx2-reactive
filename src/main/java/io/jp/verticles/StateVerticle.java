package io.jp.verticles;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.EventBus;
import io.jp.message.State;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class StateVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(StateVerticle.class);

	private Map<Integer, State> states = new HashMap<>();

	@Override
	public void start() {
		registerStateBus();
		initPeriodicalStateCheck();
	}

	private void initPeriodicalStateCheck() {
		vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), time -> checkStates());
	}

	private void checkStates() {
		if (states.values().contains(State.ONLINE)) {
			vertx.eventBus().send(EventBus.CONSUMER_CONTROL, true);
			LOG.info("At least one online worker -> no action");
		} else {
			LOG.info("All workers down -> Stop consumer");
			vertx.eventBus().send(EventBus.CONSUMER_CONTROL, false);
		}
	}

	private void registerStateBus() {
		vertx.eventBus().consumer("state", this::handleState);
	}

	private void handleState(Message<JsonObject> event) {
		LOG.info("Received StateMessage: " + event.body());
		states.put(event.body().getInteger("id"), State.byName(event.body().getString("state")));
		checkStates();
	}

}
