package com.ulfric.dragoon.rethink.response;

public class ResponseHelper {

	public static boolean changedData(Response response) {
		return isPositive(response.getInserted())
				|| isPositive(response.getReplaced())
				|| isPositive(response.getDeleted());
	}

	private static boolean isPositive(Integer value) {
		return value != null && value > 0;
	}

	private ResponseHelper() {
	}

}
