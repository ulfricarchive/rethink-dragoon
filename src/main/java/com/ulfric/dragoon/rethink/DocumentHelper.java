package com.ulfric.dragoon.rethink;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.ulfric.commons.value.UniqueIdHelper;

public class DocumentHelper {

	public static UUID getUniqueId(Document document) {
		if (document == null) {
			return null;
		}

		Location location = document.getLocation();
		if (location == null) {
			return null;
		}

		String key = location.getKey();
		if (StringUtils.isEmpty(key)) {
			return null;
		}

		return UniqueIdHelper.parseUniqueId(key);
	}

	private DocumentHelper() {
	}

}
