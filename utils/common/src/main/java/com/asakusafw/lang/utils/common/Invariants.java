/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.utils.common;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Utilities for program invariants.
 * @since 0.4.0
 */
public final class Invariants {

    private Invariants() {
        return;
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @return the value
     * @throws IllegalStateException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value) {
        require(value != null);
        return value;
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @param message the exception message
     * @return the value
     * @throws IllegalStateException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value, String message) {
        require(value != null, message);
        return value;
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @param messageSupplier the exception message supplier
     * @return the value
     * @throws IllegalStateException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value, Supplier<?> messageSupplier) {
        require(value != null, messageSupplier);
        return value;
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @throws IllegalStateException if the condition is {@code false}
     */
    public static void require(boolean condition) {
        if (condition == false) {
            throw new IllegalStateException();
        }
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @param message the exception message
     * @throws IllegalStateException if the condition is {@code false}
     */
    public static void require(boolean condition, String message) {
        if (condition == false) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @param messageSupplier the exception message supplier
     * @throws IllegalStateException if the condition is {@code false}
     */
    public static void require(boolean condition, Supplier<?> messageSupplier) {
        if (condition == false) {
            Object m = messageSupplier.get();
            throw new IllegalStateException(m == null ? null : m.toString());
        }
    }

    /**
     * Raises an exception if the target value is {@link Optional#empty()}.
     * @param <T> the value type
     * @param optional the optional object
     * @return the value
     * @throws IllegalStateException if the value is {@code empty}
     */
    public static <T> T requirePresent(Optional<? extends T> optional) {
        return optional.orElseThrow(IllegalStateException::new);
    }

    /**
     * Assumes that the action does not throw any exceptions.
     * @param <T> the return type
     * @param action the target action
     * @return the result of {@link Callable#call() callable.call()}
     * @throws IllegalStateException if the action throws a checked exception
     */
    public static <T> T safe(Callable<T> action) {
        Objects.requireNonNull(action);
        try {
            return action.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Assumes that the action does not throw any exceptions.
     * @param action the target action
     * @throws IllegalStateException if the action throws a checked exception
     */
    public static void safe(RunnableWithException<?> action) {
        Objects.requireNonNull(action);
        try {
            action.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
