package com.ulfric.dragoon.rethink;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.application.Container;
import com.ulfric.dragoon.conf4j.Settings;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.rethink.jms.RethinkSubscriber;
import com.ulfric.dragoon.vault.Secret;

import javax.jms.MessageConsumer;

public class RethinkContainer extends Container {

	@Secret("rethinkdb/username") // TODO configurable to not use vault
	private String username;

	@Secret("rethinkdb/password") // TODO configurable to not use vault
	private String password;

	@Settings("rethinkdb")
	private RethinkConfig settings;

	@Inject
	private ObjectFactory factory;

	private Connection connection;

	public RethinkContainer() {
		install(DatabaseExtension.class);

		bindRethink();
		bindConnection();
		bindRethinkSubscriber();

		addShutdownHook(this::closeConnection);
	}

	private void bindRethink() {
		factory.bind(RethinkDB.class).toValue(RethinkDB.r);
	}

	private void bindConnection() {
		factory.bind(Connection.class).toLazy(parameters -> {
			RethinkDB rethink = factory.request(RethinkDB.class);
			connection = rethink.connection()
				.hostname("localhost") // TODO configurable - localhost for local proxy (or just hosted locally
				.user(username, password)
				.db(settings.defaultDatabase())
				.timeout(settings.timeout())
				.connect(); // TODO retries
			return connection;
		});
	}

	private void bindRethinkSubscriber() {
		factory.bind(RethinkSubscriber.class).toLazy(parameters -> {
			MessageConsumer backing = factory.request(MessageConsumer.class, parameters);
			return new RethinkSubscriber(backing);
		});
	}

	private void closeConnection() {
		if (connection != null) {
			connection.close();
		}
	}

}
