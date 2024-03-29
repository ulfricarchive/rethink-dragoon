package com.ulfric.dragoon.rethink;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface Database {

	String value() default RethinkContainer.DEFAULT_KEY;

	String table() default RethinkContainer.DEFAULT_KEY;

}
