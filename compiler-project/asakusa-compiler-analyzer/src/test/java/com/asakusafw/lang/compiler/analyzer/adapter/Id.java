package com.asakusafw.lang.compiler.analyzer.adapter;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Element ID.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {

    /**
     * the identity token.
     */
    String value();
}
