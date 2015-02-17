package com.asakusafw.lang.compiler.analyzer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mock operator annotation.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MockOperator {

    /**
     * operator ID.
     */
    String id() default "";

    /**
     * method parameter names.
     */
    String[] parameters() default {};
}
