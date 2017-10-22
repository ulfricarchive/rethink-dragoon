package com.ulfric.dragoon.rethink;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

final class UpdatableInstance<T extends Document> implements Instance<T> {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
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

	public boolean isAbsent() {
		return get() == null;
	}

	public void lockWrite() {
		lock.writeLock().lock();
	}

	public void unlockWrite() {
		lock.writeLock().unlock();
	}

	public void lockRead() {
		lock.readLock().lock();
	}

	public void unlockRead() {
		lock.readLock().unlock();
	}

	@Override
	public void addListener(Consumer<T> listener) {
		Objects.requireNonNull(listener, "listener");

		callbacks.add(listener);
	}

}
