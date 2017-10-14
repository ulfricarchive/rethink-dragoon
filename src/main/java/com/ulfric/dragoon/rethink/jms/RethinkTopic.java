package com.ulfric.dragoon.rethink.jms;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.ulfric.dragoon.activemq.Topic;
import com.ulfric.dragoon.stereotype.Stereotype;

@Retention(RUNTIME)
@Target(FIELD)
@Topic("rethinkdb")
@Stereotype
public @interface RethinkTopic {

}
