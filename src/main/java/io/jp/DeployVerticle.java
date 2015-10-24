package io.jp;

import org.vertx.java.platform.Verticle;

public class DeployVerticle extends Verticle {

	@Override
	public void start() {
		container.deployVerticle(StateVerticle.class.getName());
		container.deployVerticle(HttpVerticle.class.getName());
		container.deployVerticle(ConsumerVerticle.class.getName());
		// container.deployWorkerVerticle(SinkVerticle.class.getName(), 2);
		container.deployWorkerVerticle(HbaseVerticle.class.getName(), 2);
	}

}
