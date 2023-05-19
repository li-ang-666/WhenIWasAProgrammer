package com.liang.repair.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//一般来说自己写的注解都是RUNTIME
@Retention(RetentionPolicy.RUNTIME)
//类注解
@Target(ElementType.TYPE)
public @interface Prop {
    String key1() default "value1";

    String key2() default "value2";
}
