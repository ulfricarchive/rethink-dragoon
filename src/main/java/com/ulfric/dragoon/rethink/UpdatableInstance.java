package com.ulfric.dragoon.rethink;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

final class UpdatableInstance<T extends Document> implements Instance<T> {

	private final List<Consumer<T>> callbacks = new ArrayList<>();
	private T value;

	@Override
	public T get() {
		return value;
	}

	public void update(T newValue) {
		this.value = newValue;
		callbacks.forEach(consumer -> consumer.accept(newValue));
	}

	@Override
	public void addListener(Consumer<T> listener) {
		Objects.requireNonNull(listener, "listener");

		callbacks.add(listener);
	}

}
