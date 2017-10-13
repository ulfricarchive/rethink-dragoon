package com.ulfric.dragoon.rethink;

public abstract class RuntimeStore<T extends Document> {

	protected abstract Store<T> store();

}
