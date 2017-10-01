package com.ulfric.dragoon.rethink;

import com.ulfric.conf4j.ConfigurationBean;

public interface RethinkConfig extends ConfigurationBean {

	long timeout();

	String defaultDatabase();

	String defaultTable();

}
