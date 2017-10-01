package com.ulfric.dragoon.rethink.jms;

import com.ulfric.dragoon.activemq.event.EventSubscriber;
import com.ulfric.dragoon.rethink.Location;

import javax.jms.MessageConsumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RethinkSubscriber extends EventSubscriber<DocumentUpdateEvent> {

	private final Map<Location, List<Consumer<DocumentUpdateEvent>>> listeners = new HashMap<>();

	public RethinkSubscriber(MessageConsumer consumer) {
		super(consumer, DocumentUpdateEvent.class);

		setListener(new RethinkListener());
	}

	public void addListener(Location location, Consumer<DocumentUpdateEvent> listener) {
		listeners.computeIfAbsent(location, ignore -> new ArrayList<>()).add(listener);
	}

	public void removeListener(Location location, Consumer<DocumentUpdateEvent> listener) {
		List<Consumer<DocumentUpdateEvent>> consumers = this.listeners.get(location);

		if (consumers == null) {
			return;
		}

		consumers.remove(listener);
	}

	public void clearListeners(Location location) {
		listeners.remove(location);
	}

	private class RethinkListener implements Consumer<DocumentUpdateEvent> {
		@Override
		public void accept(DocumentUpdateEvent event) {
			Location location = event.getLocation();

			if (location == null) {
				return;
			}

			List<Consumer<DocumentUpdateEvent>> consumers = listeners.get(location);

			if (consumers == null) {
				return;
			}

			consumers.forEach(consumer -> consumer.accept(event));
		}
	}

}
