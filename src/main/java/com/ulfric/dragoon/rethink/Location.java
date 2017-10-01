package com.ulfric.dragoon.rethink;

import java.util.Objects;

public class Location {

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private String database;
		private String table;
		private Object key;

		protected Builder() {
		}

		public Location build() {
			return new Location(database, table, key);
		}

		public Builder database(String database) {
			this.database = database;
			return this;
		}

		public Builder table(String table) {
			this.table = table;
			return this;
		}

		public Builder key(Object key) {
			this.key = key;
			return this;
		}
	}

	private final String database;
	private final String table;
	private final Object key;
	private int hashCode;
	private boolean hashed;

	protected Location(String database, String table, Object key) {
		this.database = database;
		this.table = table;
		this.key = key;
	}

	public final String getDatabase() {
		return database;
	}

	public final String getTable() {
		return table;
	}

	public Object getKey() {
		return key;
	}

	@Override
	public boolean equals(Object object) {
		if (object == this) {
			return true;
		}

		if (object == null) {
			return false;
		}

		if (object.getClass() != this.getClass()) {
			return false;
		}

		Location that = (Location) object;

		return Objects.equals(database, that.database) &&
				Objects.equals(table, that.table) &&
				Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		if (hashed) {
			return hashCode;
		}

		hashCode = Objects.hash(database, table, key);
		hashed = true;
		return hashCode;
	}

}
