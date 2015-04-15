package com.asakusafw.lang.compiler.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents the target implementation is exclusive in the same API category.
 * That is, the target implementation cannot run with other exclusive implementations.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Exclusive {

    /**
     * whether the target API is optional.
     */
    boolean optional() default false;
}
