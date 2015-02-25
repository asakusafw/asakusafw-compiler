/**
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.cli;

import java.util.Collections;
import java.util.Iterator;

/**
 * Represents a value with default.
 * @param <T> the value type
 */
public class ValueHolder<T> implements Holder<T> {

    private T value;

    /**
     * Creates a new instance.
     */
    public ValueHolder() {
        this.value = null;
    }

    /**
     * Creates a new instance with default value.
     * @param defaultValue the default value
     */
    public ValueHolder(T defaultValue) {
        this.value = defaultValue;
    }

    /**
     * Sets the value only if the specified one is not {@code null}.
     * @param valueOrNull a new value, or {@code null}
     */
    public void set(T valueOrNull) {
        if (valueOrNull != null) {
            this.value = valueOrNull;
        }
    }

    /**
     * Returns the value.
     * @return the value
     */
    public T get() {
        return this.value;
    }

    @Override
    public boolean isEmpty() {
        return value == null;
    }

    @Override
    public Iterator<T> iterator() {
        if (value == null) {
            return Collections.<T>emptySet().iterator();
        } else {
            return Collections.singleton(value).iterator();
        }
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
