package com.ulfric.dragoon.rethink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Json;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.net.Cursor;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.activemq.event.EventPublisher;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.extension.intercept.asynchronous.Asynchronous;
import com.ulfric.dragoon.extension.postconstruct.PostConstruct;
import com.ulfric.dragoon.reflect.Instances;
import com.ulfric.dragoon.rethink.jms.DocumentUpdateEvent;
import com.ulfric.dragoon.rethink.jms.RethinkSubscriber;
import com.ulfric.dragoon.rethink.jms.RethinkTopic;
import com.ulfric.dragoon.rethink.response.Response;
import com.ulfric.dragoon.rethink.response.ResponseHelper;

public class Store<T extends Document> implements AutoCloseable { // TODO unit tests

	private final Class<T> type;
	private final Location defaultLocation;
	private final Map<Location, Consumer<DocumentUpdateEvent>> listeners = new ConcurrentHashMap<>(1);
	private final Map<Location, UpdatableInstance<T>> cache = new ConcurrentHashMap<>(2);

	@Inject
	private ObjectFactory factory;

	@Inject(optional = true)
	private Logger logger;

	@Inject
	private Gson gson;

	@Inject
	private RethinkDB rethinkdb;

	@Inject(optional = true)
	@RethinkTopic
	private EventPublisher<DocumentUpdateEvent> publisher;

	@Inject(optional = true)
	@RethinkTopic
	private RethinkSubscriber subscriber;

	@Inject
	private ConnectionFactory connection;

	public Store(Class<T> type, Location defaultLocation) {
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(defaultLocation, "defaultLocation");

		this.type = type;
		this.defaultLocation = defaultLocation;
	}

	@PostConstruct
	private void prepareRethinkDb() {
		createDatabase();
		createTable();
	}

	private void createDatabase() {
		try {
			String database = defaultDatabase();

			Object created = rethinkdb.dbCreate(defaultDatabase()).run(connection.get());
			Response response = response(created);
			if (ResponseHelper.changedData(response)) {
				info(String.format("Created database '%s'", database));
			} else {
				info(String.format("Database '%s' already exists, but no exception was thrown", database));
			}
		} catch (ReqlOpFailedError databaseAlreadyExists) {
			info(databaseAlreadyExists.getMessage());
		}
	}

	private void createTable() {
		try {
			String database = defaultDatabase();
			String table = defaultTable();

			Object created = rethinkdb.db(database).tableCreate(table).run(connection.get());
			Response response = response(created);
			if (ResponseHelper.changedData(response)) {
				info(String.format("Created table '%s' in database '%s'", table, database));
			} else {
				info(String.format("Table '%s' already exists in database '%s', but no exception was thrown", table, database));
			}
		} catch (ReqlOpFailedError tableAlreadyExists) {
			info(tableAlreadyExists.getMessage());
		}
	}

	public T getFromLocalCache(Location location) {
		Instance<T> instance = cache.get(location);
		return instance == null ? null : instance.get();
	}

	public CompletableFuture<Instance<T>> get(Location key) {
		Location location = location(key);

		UpdatableInstance<T> instance = instance(location);

		boolean isAbsent;
		instance.lockRead();
		try {
			isAbsent = instance.isAbsent();
		} finally {
			instance.unlockRead();
		}

		if (isAbsent) {
			instance.lockWrite();

			isAbsent = instance.isAbsent();
			if (isAbsent) {
				return getFromDatabaseBypassingCache(location)
						.thenAccept(value -> { // TODO error handling
							if (value == null) {
								value = Instances.instance(type); // TODO is this what we really want?
								value.setLocation(location);
							}
							instance.update(value);
							instance.unlockWrite();
						})
						.thenApply(ignore -> instance);
			}

			instance.unlockWrite();
		}

		return CompletableFuture.completedFuture(instance);
	}

	@Asynchronous
	public CompletableFuture<T> getFromDatabaseBypassingCache(Location location) {
		T value = readFromDatabase(location);
		if (value != null && value.getLocation() == null) {
			value.setLocation(location);
		}

		return CompletableFuture.completedFuture(value);
	}

	@Asynchronous
	public CompletableFuture<List<Instance<T>>> listAllFromDatabase() { // TODO cleanup
		Cursor<Map<String, Object>> table = databaseTable(defaultLocation)
			.run(connection.get());

		List<Instance<T>> instances = new ArrayList<>(table.bufferedSize()); // TODO make sure bufferedSize returns what I think it does

		Location.Builder builder = defaultLocation.toBuilder();

		for (Map<String, Object> document : table) {
			Location location = builder.key(document.get("id")).build();

			UpdatableInstance<T> instance = instance(location);
			instance.lockWrite();
			try {
				T value = readTypeFromJson(document);
				instance.update(value);
			} finally {
				instance.unlockWrite();
			}

			instances.add(instance);
		}

		return CompletableFuture.completedFuture(instances);
	}

	private T readFromDatabase(Location location) {
		Map<String, Object> document = rethinkdb.db(location.getDatabase())
				.table(location.getTable())
				.get(location.getKey())
				.run(connection.get());

		T value = readTypeFromJson(document);
		if (value != null) {
			value.setLocation(location);
		}
		return value;
	}

	private T readTypeFromJson(Map<String, Object> document) {
		if (document == null) {
			return null;
		}

		JsonElement json = gson.toJsonTree(document);
		return gson.fromJson(json, type);
	}

	public CompletableFuture<Response> insert(T value) {
		return run(this::insert, value);
	}

	private Response insert(Location location, T value) {
		Object result = databaseTable(location)
				.get(location.getKey())
				.replace(json(location, value))
				.run(connection.get());

		return response(result);
	}

	public CompletableFuture<Response> delete(Location location) {
		T dummy = Instances.instance(type); // TODO better solution
		dummy.setLocation(location);
		return delete(dummy);
	}

	public CompletableFuture<Response> delete(T value) {
		return this.run(this::delete, value);
	}

	private Response delete(Location location, T ignore) {
		Objects.requireNonNull(location.getKey(), "key"); // TODO is this needed? not taking chances right now

		Object result = databaseTable(location)
				.get(location.getKey())
				.delete()
				.run(connection.get());

		return response(result);
	}

	@Asynchronous
	public CompletableFuture<Response> run(BiFunction<Location, T, Response> run, T value) {
		Location location = location(value.getLocation());
		Response response = run.apply(location, value);

		if (ResponseHelper.changedData(response)) {
			notifyActiveMq(location);
		}

		return CompletableFuture.completedFuture(response);
	}

	private Table databaseTable(Location location) {
		return rethinkdb.db(location.getDatabase()).table(location.getTable());
	}

	private Json json(Location location, T value) {
		String key = location.getKey();
		if (key == null) {
			return rethinkdb.json(gson.toJson(value, type));
		}

		JsonElement jsonElement = gson.fromJson(gson.toJson(value, type), JsonElement.class);
		if (jsonElement == null) {
			jsonElement = new JsonObject();
		}
		JsonObject json = jsonElement.getAsJsonObject();
		json.addProperty("id", key);
		return rethinkdb.json(gson.toJson(json));
	}

	private Response response(Object map) {
		return gson.fromJson(gson.toJson(map), Response.class);
	}

	private void notifyActiveMq(Location location) {
		if (publisher == null) {
			return; // TODO log warning?
		}

		DocumentUpdateEvent event = new DocumentUpdateEvent();
		event.setTimestamp(System.currentTimeMillis());
		event.setLocation(location);

		publisher.send(event);
	}

	private UpdatableInstance<T> instance(Location location) {
		return cache.computeIfAbsent(location, key -> {
				UpdatableInstance<T> instance = new UpdatableInstance<>();
				addListener(key, instance);
				return instance;
			});
	}

	private void addListener(Location location, UpdatableInstance<T> instance) {
		Consumer<DocumentUpdateEvent> oldListener = listeners.put(location, event -> {
			T value = readFromDatabase(location);
			instance.lockWrite();
			try {
				instance.update(value);
			} finally {
				instance.unlockWrite();
			}
		});

		if (oldListener != null) {
			alert(gson.toJson(location) + " had a duplicate listener");
		}
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

	public void close(Location location) {
		if (location == null) {
			return;
		}

		Consumer<DocumentUpdateEvent> listener = listeners.remove(location);
		if (listener == null) {
			return;
		}

		if (subscriber != null) {
			subscriber.removeListener(location, listener);
		}

		cache.remove(location);
	}

	@Override
	public void close() {
		if (subscriber != null) {
			listeners.forEach(subscriber::removeListener);
		}
		listeners.clear();
		cache.clear();
	}

	private void info(String message) {
		if (logger != null) {
			logger.info(message);
		}
	}

	private void alert(String message) {
		if (logger != null) {
			logger.severe(message);
		}
	}

}
