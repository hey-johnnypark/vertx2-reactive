package io.jp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

@SuppressWarnings("rawtypes")
public class ConsumerVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerVerticle.class);

	private static long NOT_RUNNING = -1;

	private long periodicTimerId = NOT_RUNNING;

	private static AtomicLong CNT = new AtomicLong();

	private void startConsuming() {
		if (periodicTimerId == NOT_RUNNING) {
			periodicTimerId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1), event -> {
				JsonObject payload = new JsonObject()
						.put("key", Long.toString(CNT.getAndIncrement()))
						.put("val", "foobar");
				vertx.eventBus().send(EventBus.HBASE_PUT, payload);
				LOG.info("Sent {}", payload);
			});
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

		vertx.eventBus().consumer(EventBus.CONSUMER_CONTROL, msg -> {
			LOG.info("Received control info: {}", msg.body());
			if ((boolean) msg.body()) {
				startConsuming();
			} else {
				stopConsuming();
			}

		});
		LOG.info("{} started", getClass().getName());
	}

}
