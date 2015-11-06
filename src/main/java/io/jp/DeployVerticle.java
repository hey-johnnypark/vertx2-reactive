package io.jp;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;
import org.vertx.java.platform.Verticle;

public class DeployVerticle extends Verticle {

	private Map<String, Object> deployMap = new HashMap<>();

	private static URL[] CLASSES;

	static {
		try {
			CLASSES = new URL[] { new File(".", "target/classes").getCanonicalFile().toURL() };
		} catch (Exception e) {
		}
	}

	public class DeployHandler implements Handler<AsyncResult<String>> {

		private String verticle;

		public DeployHandler(String verticle) {
			this.verticle = verticle;
		}

		@Override
		public void handle(AsyncResult<String> event) {
			if (event.succeeded()) {
				container.logger().info("Deployed verticle " + verticle);
				deployMap.put(event.result(), verticle);
			} else {
				container.logger().error("While deploying verticle", event.cause());
			}
		}

	}

	public class UndeployHandler implements Handler<AsyncResult<Void>> {

		private String deploymentId;

		public UndeployHandler(String deploymentId) {
			this.deploymentId = deploymentId;
		}

		@Override
		public void handle(AsyncResult<Void> event) {
			if (event.succeeded()) {
				container.logger().info("Undeployed verticle with id: " + deploymentId);
				deployMap.remove(deploymentId);
			}
		}

	}

	@Override
	public void start() {
		container.deployVerticle(HttpVerticle.class.getName(), new DeployHandler(HttpVerticle.class.getName()));
		container.deployVerticle(JmsProducerVerticle.class.getName(),
				new DeployHandler(JmsProducerVerticle.class.getName()));

		vertx.eventBus().registerHandler(EventBus.DEPLOY_DEPLOY, new Handler<Message<String>>() {

			@Override
			public void handle(final Message<String> verticle) {
				container.deployVerticle(verticle.body(), new DeployHandler(verticle.body()));
			}
		});

		vertx.eventBus().registerHandler(EventBus.DEPLOY_UNDEPLOY, new Handler<Message<String>>() {

			@Override
			public void handle(Message<String> deploymentId) {
				container.undeployVerticle(deploymentId.body(), new UndeployHandler(deploymentId.body()));
			}
		});

		vertx.eventBus().registerHandler(EventBus.DEPLOY_MAP, new Handler<Message<String>>() {

			@Override
			public void handle(Message<String> deploymentId) {
				container.logger().info("MAP: " + deployMap);
				deploymentId.reply(new JsonObject(deployMap));
			}
		});

	}

	public static void main(String[] args) throws MalformedURLException, IOException, InterruptedException {

		URL url = new File(".", "target/classes").getCanonicalFile().toURL();
		PlatformManager pm = PlatformLocator.factory.createPlatformManager();

		pm.deployVerticle(DeployVerticle.class.getName(), new JsonObject(), new URL[] { url }, 1, null, null);

		Thread.sleep(TimeUnit.MINUTES.toMillis(10));
	}
}
