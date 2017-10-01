package com.ulfric.dragoon.rethink;

import com.ulfric.dragoon.extension.Extension;

public class DatabaseExtension extends Extension {

	@Override
	public <T> T transform(T value) {
		return value;
	}

}
