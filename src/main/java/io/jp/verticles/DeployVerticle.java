package io.jp.verticles;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.EventBus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class DeployVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(DeployVerticle.class);

    private Map<String, Object> deployMap = new HashMap<>();

    public class DeployHandler implements Handler<AsyncResult<String>> {

        private String verticle;

        public DeployHandler(String verticle) {
            this.verticle = verticle;
        }

        @Override
        public void handle(AsyncResult<String> event) {
            if (event.succeeded()) {
                LOG.info("Deployed verticle {}", verticle);
                deployMap.put(event.result(), verticle);
            } else {
                LOG.error("While deploying verticle", event.cause());
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
                LOG.info("Undeployed verticle with id: {}", deploymentId);
                deployMap.remove(deploymentId);
            }
        }

    }

    @Override
    public void start() throws InterruptedException {

        vertx.deployVerticle(StateVerticle.class.getName(), new DeployHandler(StateVerticle.class.getName()));
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        vertx.deployVerticle(HttpVerticle.class.getName(), new DeployHandler(HttpVerticle.class.getName()));
        vertx.deployVerticle(DummySinkVerticle.class.getName(),
            new DeployHandler(DummySinkVerticle.class.getName()));
        vertx.deployVerticle(JmsConsumerVerticle.class.getName(),
            new DeployHandler(JmsConsumerVerticle.class.getName()));
        vertx.deployVerticle(JmsProducerVerticle.class.getName(),
            new DeployHandler(JmsProducerVerticle.class.getName()));

        vertx.eventBus().consumer(EventBus.DEPLOY_DEPLOY, new Handler<Message<String>>() {

            @Override
            public void handle(final Message<String> verticle) {
                vertx.deployVerticle(verticle.body(), new DeployHandler(verticle.body()));
            }
        });

        vertx.eventBus().consumer(EventBus.DEPLOY_UNDEPLOY, new Handler<Message<String>>() {

            @Override
            public void handle(Message<String> deploymentId) {
                vertx.undeploy(deploymentId.body(), new UndeployHandler(deploymentId.body()));
            }
        });

        vertx.eventBus().consumer(EventBus.DEPLOY_MAP, new Handler<Message<String>>() {

            @Override
            public void handle(Message<String> deploymentId) {
                LOG.info("MAP: ", deployMap);
                deploymentId.reply(new JsonObject(deployMap));
            }
        });

    }

    public static void main(String[] args) throws MalformedURLException, IOException, InterruptedException {

        Vertx.vertx().deployVerticle(DeployVerticle.class.getName());

        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
    }
}
