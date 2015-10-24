package io.jp.message;

import org.vertx.java.core.json.JsonObject;

public class StateMessage extends JsonObject {

	private static final long serialVersionUID = -3623952659289872060L;

	public StateMessage withId(int id) {
		putNumber("id", id);
		return this;
	}

	public int getId() {
		return getNumber("id").intValue();
	}

	public StateMessage withState(State state) {
		putString("state", state.name());
		return this;
	}

	public State getState() {
		return State.byName(getString("state"));
	}

	@Override
	public String toString() {
		return super.toString();
	}

}
