package io.jp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class HttpVerticle extends Verticle {

	@Override
	public void start() {
		HttpServer server = vertx.createHttpServer();
		RouteMatcher routeMatcher = new RouteMatcher();

		routeMatcher.get("/sink/on", new Handler<HttpServerRequest>() {

			@Override
			public void handle(HttpServerRequest event) {
				vertx.eventBus().send(EventBus.SINK_CONTROL, true);
				event.response().setStatusCode(200);
				event.response().end();
			}
		});

		routeMatcher.get("/sink/off", new Handler<HttpServerRequest>() {

			@Override
			public void handle(HttpServerRequest event) {
				vertx.eventBus().send(EventBus.SINK_CONTROL, false);
				event.response().setStatusCode(200);
				event.response().end();
			}
		});

		server.requestHandler(routeMatcher);
		server.listen(8080, "localhost");
	}

}
