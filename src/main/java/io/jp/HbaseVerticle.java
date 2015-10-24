package io.jp;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import io.jp.message.State;
import io.jp.message.StateMessage;

public class HbaseVerticle extends Verticle {

	private static final AtomicInteger INSTANCE_ID = new AtomicInteger(0);

	private int instanceId = INSTANCE_ID.getAndIncrement();

	private byte[] TABLE = "txm:test".getBytes();

	private byte[] CF = "cf1".getBytes();

	private HConnection hbaseConn;

	private long reconnectTimer = -1;

	private class ReconnectHandler implements Handler<Long> {

		@Override
		public void handle(Long event) {
			sendState(State.CONNECTING);
			try {
				createConnection();
				vertx.cancelTimer(reconnectTimer);
				reconnectTimer = -1;
				sendState(State.ONLINE);
			} catch (IOException e) {
				container.logger().error("Reconnect failed: ", e);
			}
		}
	}

	@Override
	public void start(Future<Void> startedResult) {
		try {
			createConnection();
			sendState(State.ONLINE);
		} catch (IOException e) {
			container.logger().error(e);
			startedResult.setFailure(e);
		}
		vertx.eventBus().registerHandler(EventBus.HBASE_PUT, new Handler<Message<JsonObject>>() {

			@Override
			public void handle(Message<JsonObject> data) {
				JsonObject packet = data.body();
				container.logger().info("Put: " + packet);
				Put put = jsonToPut(packet);
				try {
					hbaseConn.getTable(TABLE).put(put);
				} catch (Exception e) {
					container.logger().error(e);
					sendState(State.OFFLINE);
					if (reconnectTimer == -1) {
						reconnectTimer = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(10), new ReconnectHandler());
					}
					data.reply(e);
				}
			}

		});

	}

	private void createConnection() throws IOException {
		if (hbaseConn != null) {
			forceClose();
			hbaseConn = null;
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.client.retries.number", "3");
		hbaseConn = HConnectionManager.createConnection(conf);
		hbaseConn.getTable(TABLE).exists(new Get(Bytes.toBytes("FOOBAR")));
	}

	private void forceClose() {
		try {
			hbaseConn.close();
		} catch (IOException e) {
		}
	}

	private Put jsonToPut(JsonObject packet) {
		Put put = new Put(Bytes.toBytes(String.format("%10s", packet.getString("key"))));
		for (String key : packet.getFieldNames()) {
			if (key == "key")
				continue;
			put.add(CF, Bytes.toBytes(key), Bytes.toBytes(packet.getString(key)));
		}
		return put;
	}

	private void sendState(State state) {
		vertx.eventBus().send("state", new StateMessage().withId(instanceId).withState(state));
	}

}
