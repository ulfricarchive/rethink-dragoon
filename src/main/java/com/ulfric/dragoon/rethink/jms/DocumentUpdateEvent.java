package com.ulfric.dragoon.rethink.jms;

import com.ulfric.dragoon.activemq.event.Event;
import com.ulfric.dragoon.rethink.Location;

public class DocumentUpdateEvent extends Event {

	private Location location;
	private Long timestamp;

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

}