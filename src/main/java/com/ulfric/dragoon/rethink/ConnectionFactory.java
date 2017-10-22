package com.ulfric.dragoon.rethink;

import java.util.function.Supplier;

import com.rethinkdb.net.Connection;

public interface ConnectionFactory extends Supplier<Connection> {

}
