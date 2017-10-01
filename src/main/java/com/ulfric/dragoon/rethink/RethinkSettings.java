package com.ulfric.dragoon.rethink;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.ulfric.dragoon.conf4j.Settings;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
@Settings("rethinkdb")
public @interface RethinkSettings {

}
