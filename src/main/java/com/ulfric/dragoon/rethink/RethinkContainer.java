package com.ulfric.dragoon.rethink;

import java.util.logging.Logger;

import javax.jms.MessageConsumer;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.application.Container;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.rethink.jms.RethinkSubscriber;
import com.ulfric.dragoon.vault.Secret;

public class RethinkContainer extends Container { // TODO aop logging

	@Secret("rethinkdb/username") // TODO configurable to not use vault
	private String username;

	@Secret("rethinkdb/password") // TODO configurable to not use vault
	private String password;

	@RethinkSettings
	private RethinkConfig settings;

	@Inject
	private ObjectFactory factory;

	@Inject(optional = true)
	private Logger logger;

	private Connection connection;

	public RethinkContainer() {
		install(DatabaseExtension.class);

		addBootHook(this::registerBindings);

		addShutdownHook(this::unregisterBindings);
		addShutdownHook(this::closeConnection);
	}

	private void registerBindings() {
		bindRethink();
		bindConnection();
		bindRethinkSubscriber();
	}

	private void unregisterBindings() {
		factory.bind(RethinkDB.class).toNothing();
		factory.bind(Connection.class).toNothing();
		factory.bind(RethinkSubscriber.class).toNothing();
	}

	private void bindRethink() {
		factory.bind(RethinkDB.class).toValue(RethinkDB.r);
	}

	private void bindConnection() {
		factory.bind(Connection.class).toLazy(parameters -> {
			
			log("Connecting to rethinkdb at %s with timeout of %d seconds", settings.host(), settings.timeout());

			RethinkDB rethink = factory.request(RethinkDB.class);
			connection = rethink.connection()
				.hostname(settings.host()) // TODO configurable - localhost for local proxy (or just hosted locally
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
		if (connection != null && connection.isOpen()) {
			connection.close();
		}
	}

	private void log(String message, Object... format) {
		if (logger != null) {
			logger.info(String.format(message, format));
		}
	}

}
