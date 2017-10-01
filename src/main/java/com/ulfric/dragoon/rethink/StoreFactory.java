package com.ulfric.dragoon.rethink;

import com.ulfric.dragoon.Factory;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.Parameters;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.qualifier.GenericQualifier;
import com.ulfric.dragoon.qualifier.Qualifier;
import com.ulfric.dragoon.reflect.Instances;
import com.ulfric.dragoon.stereotype.Stereotypes;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class StoreFactory implements Factory {

	static final String DEFAULT_KEY = "<default>";

	@Inject
	private ObjectFactory creator;

	@RethinkSettings
	private RethinkConfig settings;

	@Override
	public <T> T request(Class<T> type) {
		return request(type, Parameters.EMPTY);
	}

	@Override
	public <T> T request(Class<T> type, Parameters parameters) {
		Qualifier qualifier = parameters.getQualifier();

		Database database = Stereotypes.getFirst(qualifier, Database.class);
		Location defaultLocation = Location.builder()
				.database(database.value().replace(DEFAULT_KEY, settings.defaultDatabase()))
				.table(database.table().replace(DEFAULT_KEY, settings.defaultTable()))
				.build();

		Class<?> storeType = getStoreType(qualifier);

		return Instances.instance(type, storeType, defaultLocation);
	}

	private Class<?> getStoreType(Qualifier qualifier) {
		if (qualifier instanceof GenericQualifier) {
			Type genericType = ((GenericQualifier) qualifier).getGenericType();

			if (genericType instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) genericType;
				return (Class<?>) parameterizedType.getActualTypeArguments()[0]; // TODO validate type arguments, improve this whole line
			}
		}

		return qualifier.getType();
	}

}
