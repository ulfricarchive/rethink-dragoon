package com.ulfric.dragoon.rethink;

import java.util.Objects;

import com.rethinkdb.net.Connection;

public class ReconnectingConnectionFactory implements ConnectionFactory {

	private final Connection connection;
	private final Object lock = new Object();

	public ReconnectingConnectionFactory(Connection connection) {
		Objects.requireNonNull(connection, "connection");

		this.connection = connection;
	}

	@Override
	public Connection get() {
		if (!connection.isOpen()) {
			synchronized(lock) {
				if (!connection.isOpen()) {
					connection.reconnect();
				}
			}
		}

		return connection;
	}

}
