package io.jp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
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

		routeMatcher.get("/deploy/map", new Handler<HttpServerRequest>() {

			@Override
			public void handle(final HttpServerRequest request) {
				vertx.eventBus().send(EventBus.DEPLOY_MAP, "", new Handler<Message<JsonObject>>() {

					@Override
					public void handle(Message<JsonObject> event) {
						request.response().setChunked(true);
						request.response().setStatusCode(200);
						request.response().write(event.body().toString());
						request.response().end();
					}
				});

			}
		});

		routeMatcher.get("/deploy/:verticle", new Handler<HttpServerRequest>() {

			@Override
			public void handle(HttpServerRequest event) {
				vertx.eventBus().send(EventBus.DEPLOY_DEPLOY, event.params().get("verticle"));
				event.response().setStatusCode(200);
				event.response().end();
			}
		});

		routeMatcher.get("/undeploy/:verticle", new Handler<HttpServerRequest>() {

			@Override
			public void handle(HttpServerRequest event) {
				vertx.eventBus().send(EventBus.DEPLOY_UNDEPLOY, event.params().get("verticle"));
				event.response().setStatusCode(200);
				event.response().end();
			}
		});

		server.requestHandler(routeMatcher);
		server.listen(8080, "localhost");
	}

}
