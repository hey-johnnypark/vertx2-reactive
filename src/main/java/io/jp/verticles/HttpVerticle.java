package io.jp.verticles;

import io.jp.EventBus;
import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class HttpVerticle extends AbstractVerticle {

	@Override
	public void start() {
		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());
		router.get("/sink/on").handler(this::sinkOn);
		router.get("/sink/off").handler(this::sinkOff);
		router.get("/deploy/map").handler(this::deployMap);
		router.get("/deploy/:verticle").handler(this::deploy);
		router.get("/undeploy/:verticle").handler(this::undeploy);
		vertx.createHttpServer().requestHandler(router::accept).listen(8080);
	}

	private void sinkOn(RoutingContext routingContext) {
		vertx.eventBus().send(EventBus.SINK_CONTROL, true);
		routingContext.response().setStatusCode(200);
		routingContext.response().end();
	}

	private void sinkOff(RoutingContext routingContext) {
		vertx.eventBus().send(EventBus.SINK_CONTROL, false);
		routingContext.response().setStatusCode(200);
		routingContext.response().end();
	}

	private void deployMap(RoutingContext routingContext) {
		vertx.eventBus().send(EventBus.DEPLOY_MAP, "", reply -> {
			routingContext.response().setChunked(true);
			routingContext.response().setStatusCode(200);
			routingContext.response().write(reply.result().body().toString());
			routingContext.response().end();
		});
	}

	private void deploy(RoutingContext routingContext) {
		vertx.eventBus().send(EventBus.DEPLOY_DEPLOY, routingContext.request().params().get("verticle"));
		routingContext.response().setStatusCode(200);
		routingContext.response().end();
	}

	private void undeploy(RoutingContext routingContext) {
		vertx.eventBus().send(EventBus.DEPLOY_UNDEPLOY, routingContext.request().params().get("verticle"));
		routingContext.response().setStatusCode(200);
		routingContext.response().end();
	}

}
