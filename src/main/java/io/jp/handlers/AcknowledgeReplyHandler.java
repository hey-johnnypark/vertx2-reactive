package io.jp.handlers;

import java.util.Objects;

import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.ReplyException;

public class AcknowledgeReplyHandler implements Handler<AsyncResult<io.vertx.core.eventbus.Message<Boolean>>> {

	private static Logger LOG = LoggerFactory.getLogger(AcknowledgeReplyHandler.class);

	private Message message;

	public AcknowledgeReplyHandler(Message message) {
		this.message = Objects.requireNonNull(message);
	}

	private void ackMessages(Message message) {
		try {
			message.acknowledge();
		} catch (JMSException e) {
			LOG.error("While acknowledging message: ", e);
		}
	}

	@Override
	public void handle(AsyncResult<io.vertx.core.eventbus.Message<Boolean>> result) {
		if (result.cause() != null && result.cause() instanceof ReplyException) {
			LOG.info("Request for message {} timed out: ", message);
		} else if (result.failed()) {
			LOG.info("Request for message {} failed: ", message, result.cause());
		} else {
			LOG.info("Request for message {} succeeded -> Acknowledge", message);
			ackMessages(message);
		}

	}

}
