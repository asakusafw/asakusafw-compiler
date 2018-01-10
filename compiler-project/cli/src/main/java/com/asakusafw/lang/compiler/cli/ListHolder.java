/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a list.
 * @param <T> the value type
 */
public class ListHolder<T> implements Holder<T> {

    private final List<T> value;

    /**
     * Creates a new instance.
     */
    public ListHolder() {
        this.value = new ArrayList<>();
    }

    /**
     * Adds a value only if the specified one is not {@code null}.
     * @param valueOrNull a new value, or {@code null}
     */
    public void add(T valueOrNull) {
        if (valueOrNull != null) {
            this.value.add(valueOrNull);
        }
    }

    /**
     * Adds a values.
     * @param values the values
     */
    public void addAll(Iterable<? extends T> values) {
        for (T element : values) {
            add(element);
        }
    }

    /**
     * Returns the value.
     * @return the value
     */
    public List<T> get() {
        return this.value;
    }

    /**
     * Returns the element.
     * @param index element index
     * @return the element
     */
    public T get(int index) {
        return this.value.get(index);
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return value.iterator();
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
