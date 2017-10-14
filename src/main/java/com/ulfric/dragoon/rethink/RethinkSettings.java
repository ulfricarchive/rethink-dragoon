package com.ulfric.dragoon.rethink;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.ulfric.dragoon.conf4j.Settings;
import com.ulfric.dragoon.stereotype.Stereotype;

@Retention(RUNTIME)
@Target(FIELD)
@Settings("rethinkdb")
@Stereotype
public @interface RethinkSettings {

}
