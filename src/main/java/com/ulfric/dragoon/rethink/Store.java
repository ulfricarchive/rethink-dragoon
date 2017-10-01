package com.ulfric.dragoon.rethink;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Json;
import com.rethinkdb.net.Connection;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.activemq.event.EventPublisher;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.extension.intercept.asynchronous.Asynchronous;
import com.ulfric.dragoon.rethink.jms.DocumentUpdateEvent;
import com.ulfric.dragoon.rethink.jms.RethinkSubscriber;
import com.ulfric.dragoon.rethink.jms.RethinkTopic;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class Store<T extends Document> implements AutoCloseable { // TODO unit tests

	private static final Gson GSON = new Gson();

	private final Class<T> type;
	private final Location defaultLocation;
	private final Map<Location, Consumer<DocumentUpdateEvent>> listeners = new ConcurrentHashMap<>(1);
	private final Map<Location, Instance<T>> cache = new ConcurrentHashMap<>(2);

	@Inject
	private ObjectFactory factory;

	@Inject(optional = true)
	private Logger logger;

	@Inject
	private RethinkDB rethinkdb;

	@Inject
	@RethinkTopic
	private EventPublisher<DocumentUpdateEvent> publisher;

	@Inject
	@RethinkTopic
	private RethinkSubscriber subscriber;

	@Inject
	private Connection connection;

	public Store(Class<T> type, Location defaultLocation) {
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(defaultLocation, "defaultLocation");

		this.type = type;
		this.defaultLocation = defaultLocation;
	}

	public void close(T value) {
		if (value == null) {
			return;
		}

		close(value.getLocation());
	}

	public void close(Location location) {
		if (location == null) {
			return;
		}

		Consumer<DocumentUpdateEvent> listener = listeners.remove(location);
		if (listener == null) {
			return;
		}

		subscriber.removeListener(location, listener);
	}

	@Override
	public void close() {
		listeners.forEach(subscriber::removeListener);
		listeners.clear();
		cache.clear();
	}

	@Asynchronous
	public CompletableFuture<Instance<T>> get(Location location) {
		location = location(location);

		Instance<T> instance = cache.computeIfAbsent(location, this::getFromDatabase);
		return CompletableFuture.completedFuture(instance);
	}

	private Instance<T> getFromDatabase(Location location) {
		UpdatableInstance<T> instance = new UpdatableInstance<>();
		T value = GSON.fromJson(readDatabase(location), type);
		instance.update(value);

		createListener(location, instance);

		return instance;
	}

	private void createListener(Location location, UpdatableInstance<T> instance) {
		Consumer<DocumentUpdateEvent> oldListener = listeners.put(location, event -> {
			JsonElement json = readDatabase(location);
			T value = GSON.fromJson(json, type);
			value.setLocation(location);
			instance.update(value);
		});

		if (oldListener != null) {
			alert(GSON.toJson(location) + " had a duplicate listener");
		}
	}

	private JsonElement readDatabase(Location location) {
		Map<String, Object> json = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.get(location.getKey())
				.run(connection);

		return GSON.toJsonTree(json);
	}

	public CompletableFuture<Response> insert(T value) {
		return run(this::insert, value);
	}

	private Response insert(Location location, T value) {
		Object result = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.insert(json(location, value))
				.run(connection);

		return GSON.fromJson(GSON.toJson(result), Response.class);
	}

	public CompletableFuture<Response> update(T value) {
		return run(this::update, value);
	}

	private Response update(Location location, T value) {
		Object result = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.update(json(location, value))
				.run(connection);

		return GSON.fromJson(GSON.toJson(result), Response.class);
	}

	public CompletableFuture<Response> replace(T value) {
		return run(this::replace, value);
	}

	private Response replace(Location location, T value) {
		Object result = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.replace(json(location, value))
				.run(connection);

		return GSON.fromJson(GSON.toJson(result), Response.class);
	}

	public CompletableFuture<Response> sync(Location location) {
		return run(this::sync, null);
	}

	private Response sync(Location location, T value) {
		Object result = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.sync()
				.run(connection);

		return GSON.fromJson(GSON.toJson(result), Response.class);
	}

	public CompletableFuture<Response> run(BiFunction<Location, T, Response> run, T value) {
		Location location = location(value.getLocation());
		Response response = run.apply(location, value);

		if (isPositive(response.getInserted())) {
			notifyActiveMq(location);
		}

		return CompletableFuture.completedFuture(response);
	}

	private Json json(Location location, T value) {
		Object key = location.getKey();
		if (key == null) {
			rethinkdb.json(GSON.toJson(value, type));
		}

		JsonObject json = (JsonObject) GSON.toJsonTree(value);
		json.addProperty("id", String.valueOf(key));
		return rethinkdb.json(GSON.toJson(json));
	}

	private boolean isPositive(Integer value) {
		return value != null && value > 0;
	}

	private void notifyActiveMq(Location location) {
		DocumentUpdateEvent event = new DocumentUpdateEvent();
		event.setTimestamp(System.currentTimeMillis());
		event.setLocation(location);

		publisher.send(event);
	}

	private Location location(Location location) {
		if (location == null) {
			return defaultLocation;
		}

		String database = database(location);
		String table = table(location);
		Object key = key(location);

		return Location.builder().database(database).table(table).key(key).build();
	}

	private String database(Location location) {
		if (location == null) {
			return defaultDatabase();
		}

		String database = location.getDatabase();

		if (database == null) {
			return defaultDatabase();
		}

		return database;
	}

	private String defaultDatabase() {
		return this.defaultLocation.getDatabase();
	}

	private String table(Location location) {
		if (location == null) {
			return defaultTable();
		}

		String table = location.getTable();

		if (table == null) {
			return defaultTable();
		}

		return table;
	}

	private String defaultTable() {
		return this.defaultLocation.getTable();
	}

	private Object key(Location location) {
		if (location == null) {
			return defaultkey();
		}

		Object key = location.getKey();

		if (key == null) {
			return defaultkey();
		}

		return key;
	}

	private Object defaultkey() {
		return this.defaultLocation.getKey();
	}

	private void alert(String message) {
		if (logger != null) {
			logger.severe(message);
		}
	}

}
