package com.liang.repair.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Prop {
    String key1() default "value1";

    String key2() default "value2";
}
