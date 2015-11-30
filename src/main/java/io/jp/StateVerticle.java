package io.jp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.message.State;
import io.vertx.core.AbstractVerticle;
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
		vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5), time -> {
			LOG.info("States are {}", states);
			checkStates();
		});
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
		vertx.eventBus().consumer("state", event -> {
			JsonObject json = (JsonObject) event.body();
			LOG.info("Received StateMessage: " + json);
			states.put(json.getInteger("id"), State.byName(json.getString("state")));
			checkStates();
		});
	}

}
