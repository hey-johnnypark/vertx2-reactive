package io.jp;

import org.vertx.java.platform.Verticle;

public class DeployVerticle extends Verticle {

	@Override
	public void start() {
		container.deployVerticle(HttpVerticle.class.getName());
		container.deployVerticle(ConsumerVerticle.class.getName());
		container.deployVerticle(SinkVerticle.class.getName());
	}

}
