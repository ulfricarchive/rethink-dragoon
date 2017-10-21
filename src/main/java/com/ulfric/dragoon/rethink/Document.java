package com.ulfric.dragoon.rethink;

import java.util.Objects;

public class Document {

	private transient Location location;

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	@Override
	public boolean equals(Object that) {
		if (this == that) {
			return true;
		}

		if (that == null) {
			return false;
		}

		if (that.getClass() != getClass()) { // TODO this might be a problem if any of this code is generated dynamically
			return false;
		}

		return Objects.equals(getLocation(), ((Document) that).getLocation());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getLocation());
	}

}
