package com.ulfric.dragoon.rethink.jms;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.ulfric.dragoon.activemq.Topic;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
@Topic("rethinkdb")
public @interface RethinkTopic {

}
