package com.ulfric.dragoon.rethink;

final class UpdatableInstance<T extends Document> implements Instance<T> {

	private T value;

	@Override
	public T get() {
		return value;
	}

	public void update(T newValue) {
		this.value = newValue;
	}

}
