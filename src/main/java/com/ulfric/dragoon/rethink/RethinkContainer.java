package com.ulfric.dragoon.rethink;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.MessageConsumer;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.net.Connection;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.application.Container;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.qualifier.GenericQualifier;
import com.ulfric.dragoon.qualifier.Qualifier;
import com.ulfric.dragoon.reflect.Instances;
import com.ulfric.dragoon.rethink.jms.RethinkSubscriber;
import com.ulfric.dragoon.stereotype.Stereotypes;
import com.ulfric.dragoon.vault.Secret;

public class RethinkContainer extends Container { // TODO aop logging

	static final String DEFAULT_KEY = "<default>";

	@Secret(value = "rethinkdb/username", fallbackSecret = "admin") // TODO configurable to not use vault
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
		addBootHook(this::registerBindings);
		addShutdownHook(this::unregisterBindings);

		addShutdownHook(this::closeConnection);
	}

	private void registerBindings() {
		bindRethink();
		bindConnection();
		bindRethinkSubscriber();
		bindStore();
	}

	private void unregisterBindings() {
		factory.bind(RethinkDB.class).toNothing();
		factory.bind(Connection.class).toNothing();
		factory.bind(RethinkSubscriber.class).toNothing();
		factory.bind(Store.class).toNothing();
	}

	private void bindStore() {
		factory.bind(Store.class).toFunction(parameters -> {
			Qualifier qualifier = parameters.getQualifier();

			Database database = Stereotypes.getFirst(qualifier, Database.class);
			Location defaultLocation = Location.builder()
					.database(database.value().replace(DEFAULT_KEY, settings.defaultDatabase()))
					.table(database.table().replace(DEFAULT_KEY, settings.defaultTable()))
					.build();

			Class<?> storeType = getStoreType(qualifier);

			return Instances.instance(Store.class, storeType, defaultLocation);
		});
	}

	private Class<?> getStoreType(Qualifier qualifier) {
		if (qualifier instanceof GenericQualifier) {
			Type genericType = ((GenericQualifier) qualifier).getGenericType();

			if (genericType instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) genericType;
				return (Class<?>) parameterizedType.getActualTypeArguments()[0]; // TODO validate type arguments, improve this whole line
			}
		}

		return qualifier.getType();
	}

	private void bindRethink() {
		factory.bind(RethinkDB.class).toFunction(ignore -> {
			return RethinkDB.r;
		});
	}

	private void bindConnection() {
		factory.bind(Connection.class).toLazy(parameters -> {
			
			log("Connecting to rethinkdb at %s with timeout of %d seconds", settings.host(), settings.timeout());

			RethinkDB rethink = factory.request(RethinkDB.class);
			try {
				connection = rethink.connection()
						.hostname(settings.host()) // TODO configurable - localhost for local proxy (or just hosted locally
						.user(username, password)
						.db(settings.defaultDatabase())
						.timeout(settings.timeout())
						.connect(); // TODO retries
			} catch (ReqlDriverError exception) {
				error("Failed to connect to rethinkdb", exception);
			}

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

	private void error(String message, Throwable thrown) {
		if (logger != null) {
			logger.log(Level.SEVERE, message, thrown);
		}
	}

}
