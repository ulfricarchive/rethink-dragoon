package com.ulfric.dragoon.rethink;

import com.ulfric.dragoon.Factory;
import com.ulfric.dragoon.extension.Extension;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.reflect.FieldProfile;
import com.ulfric.dragoon.reflect.LazyFieldProfile;

public class DatabaseExtension extends Extension {

	private final LazyFieldProfile fields = new LazyFieldProfile(this::createFieldProfile);

	@Inject
	private Factory parent;

	private boolean loading;

	private FieldProfile createFieldProfile() {
		loading = true;
		FieldProfile field = FieldProfile.builder()
				.setFactory(parent.request(StoreFactory.class))
				.setFlagToSearchFor(Database.class)
				.build();
		loading = false;
		return field;
	}

	@Override
	public <T> T transform(T value) {
		if (!loading) {
			fields.accept(value);
		}
		return value;
	}

}
