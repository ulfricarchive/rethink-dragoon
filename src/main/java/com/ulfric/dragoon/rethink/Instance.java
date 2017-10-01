package com.ulfric.dragoon.rethink;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface Instance<T extends Document> extends Supplier<T> {

	void addListener(Consumer<T> listener);

}
