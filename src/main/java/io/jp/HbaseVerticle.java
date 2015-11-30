package io.jp;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jp.message.State;
import io.jp.message.StateMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class HbaseVerticle extends AbstractVerticle {

	private static final Logger LOG = LoggerFactory.getLogger(HbaseVerticle.class);

	private class DataHandler implements Handler<Message<JsonObject>> {

		@Override
		public void handle(Message<JsonObject> data) {
			JsonObject packet = data.body();
			LOG.info("Put: {}", packet);
			Put put = jsonToPut(packet);
			try {
				hbaseConn.getTable(TABLE).put(put);
				data.reply(true);
			} catch (Exception e) {
				LOG.error("E: ", e);
				unRegister(this);
				data.reply(false);
				sendState(State.OFFLINE);
				if (reconnectTimer == -1) {
					reconnectTimer = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(10), new ReconnectHandler());
				}
				data.reply(e);
			}
		}

	}

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
				register(new DataHandler());
			} catch (IOException e) {
				LOG.error("Reconnect failed: ", e);
			}
		}
	}

	@Override
	public void start(Future<Void> startedResult) {
		try {
			createConnection();

			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE));
			desc.addFamily(new HColumnDescriptor(CF));

			HBaseAdmin admin = new HBaseAdmin(hbaseConn);
			if (admin.tableExists(TABLE)) {
				admin.disableTable(TABLE);
				admin.deleteTable(TABLE);
			}
			admin.createTable(desc);
			admin.close();
			vertx.eventBus().consumer(EventBus.HBASE_PUT, new DataHandler());
			sendState(State.ONLINE);
			startedResult.complete();
		} catch (IOException e) {
			LOG.error("E", e);
			startedResult.fail(e);
		}

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

		for (String key : packet.fieldNames()) {
			if (key == "key")
				continue;
			put.add(CF, Bytes.toBytes(key), Bytes.toBytes(packet.getString(key)));
		}
		return put;
	}

	private void sendState(State state) {
		vertx.eventBus().send("state", new StateMessage().withId(instanceId).withState(state));
	}

	private void register(DataHandler dataHandler) {
		vertx.eventBus().consumer(EventBus.HBASE_PUT, dataHandler);
	}

	private void unRegister(DataHandler dataHandler) {
		vertx.eventBus().consumer(EventBus.HBASE_PUT, dataHandler);
	}

}
