package io.jp.message;

public enum State {
	ONLINE, CONNECTING, OFFLINE;

	public static State byName(String name) {
		for (State type : values()) {
			if (type.name().equalsIgnoreCase(name)) {
				return type;
			}
		}
		return null;
	}
}
