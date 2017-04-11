/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Represents a JDBC target database profile.
 * @since 0.4.0
 * @version 0.4.1
 */
public final class JdbcProfile {

    private final String name;

    private final ConnectionPool connectionPool;

    private final int fetchSize;

    private final int insertSize;

    private final int maxInputConcurrency;

    private final int maxOutputConcurrency;

    private final Set<String> optimizations;

    private final Map<Class<?>, Object> extraOptions;

    JdbcProfile(Builder builder, ConnectionPool pool) {
        Arguments.requireNonNull(builder);
        this.name = builder.name;
        this.connectionPool = pool;
        this.fetchSize = builder.fetchSize;
        this.insertSize = builder.insertSize;
        this.maxInputConcurrency = builder.maxInputConcurrency;
        this.maxOutputConcurrency = builder.maxOutputConcurrency;
        this.optimizations = Arguments.freezeToSet(builder.optimizations);
        this.extraOptions = Arguments.freeze(builder.extraOptions);
    }

    /**
     * Returns the profile name.
     * @return the profile name
     */
    public String getName() {
        return name;
    }

    /**
     * Acquires a JDBC connection handle.
     * @return the acquired connection handle
     * @throws IOException if I/O error was occurred while acquiring a connection
     * @throws InterruptedException if interrupted while acquiring a connection
     */
    public ConnectionPool.Handle acquire() throws IOException, InterruptedException {
        return connectionPool.acquire();
    }

    /**
     * Returns the connection pool.
     * @return the connection pool
     * @since 0.4.1
     */
    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    /**
     * Returns the input fetch size.
     * @return the input fetch size, or empty if it is not specified
     */
    public OptionalInt getFetchSize() {
        return getOptionalSize(fetchSize);
    }

    /**
     * Returns the batch insert size.
     * @return the batch insert size, or empty if it is not specified
     */
    public OptionalInt getBatchInsertSize() {
        return getOptionalSize(insertSize);
    }

    /**
     * Returns the max number of threads for individual input operations.
     * @return max input concurrency, or empty if it is not specified
     */
    public OptionalInt getMaxInputConcurrency() {
        return getOptionalSize(maxInputConcurrency);
    }

    /**
     * Returns the max number of threads for individual output operations.
     * @return max output concurrency, or empty if it is not specified
     */
    public OptionalInt getMaxOutputConcurrency() {
        return getOptionalSize(maxOutputConcurrency);
    }

    private static OptionalInt getOptionalSize(int size) {
        return size <= 0 ? OptionalInt.empty() : OptionalInt.of(size);
    }

    /**
     * Returns the available optimization names.
     * @return the available optimization names
     */
    public Set<String> getOptimizations() {
        return optimizations;
    }

    /**
     * Returns an extra option.
     * @param <T> the option type
     * @param type the option type
     * @return the option, or empty if it is not defined
     */
    public <T> Optional<T> getOption(Class<T> type) {
        Arguments.requireNonNull(type);
        return Optionals.of(type.cast(extraOptions.get(type)));
    }

    @Override
    public String toString() {
        return String.format("JdbcProfile(%s)", getName()); //$NON-NLS-1$
    }

    /**
     * A builder of {@link JdbcProfile}.
     * @since 0.4.0
     */
    public static class Builder {

        final String name;

        int fetchSize;

        int insertSize;

        int maxInputConcurrency;

        int maxOutputConcurrency;

        final Set<String> optimizations = new LinkedHashSet<>();

        final Map<Class<?>, Object> extraOptions = new LinkedHashMap<>();

        /**
         * Creates a new instance.
         * @param name the profile name
         */
        public Builder(String name) {
            Arguments.requireNonNull(name);
            this.name = name;
        }

        /**
         * Sets a fetchSize.
         * @param newValue the fetchSize
         * @return this
         */
        public Builder withFetchSize(int newValue) {
            this.fetchSize = newValue;
            return this;
        }

        /**
         * Sets a insertSize.
         * @param newValue the insertSize
         * @return this
         */
        public Builder withInsertSize(int newValue) {
            this.insertSize = newValue;
            return this;
        }

        /**
         * Sets a maxInputConcurrency.
         * @param newValue the maxInputConcurrency
         * @return this
         */
        public Builder withMaxInputConcurrency(int newValue) {
            this.maxInputConcurrency = newValue;
            return this;
        }

        /**
         * Sets a maxOutputConcurrency.
         * @param newValue the maxOutputConcurrency
         * @return this
         */
        public Builder withMaxOutputConcurrency(int newValue) {
            this.maxOutputConcurrency = newValue;
            return this;
        }

        /**
         * Adds an option.
         * @param newValue the option
         * @return this
         */
        public Builder withOption(String newValue) {
            Arguments.requireNonNull(newValue);
            this.optimizations.add(newValue);
            return this;
        }

        /**
         * Adds options.
         * @param values the options
         * @return this
         */
        public Builder withOptions(Collection<String> values) {
            Arguments.requireNonNull(values);
            values.forEach(this::withOption);
            return this;
        }

        /**
         * Adds options.
         * @param values the options
         * @return this
         */
        public Builder withOptions(String... values) {
            Arguments.requireNonNull(values);
            return withOptions(Arrays.asList(values));
        }

        /**
         * Adds an option.
         * @param <T> the option type
         * @param value the option value
         * @return this
         */
        public <T extends Enum<T>> Builder withOption(T value) {
            Arguments.requireNonNull(value);
            return withOption(value.getDeclaringClass(), value);
        }

        /**
         * Adds an option.
         * @param <T> the option type
         * @param type the option type
         * @param value the option value
         * @return this
         */
        public <T> Builder withOption(Class<T> type, T value) {
            Arguments.requireNonNull(type);
            Arguments.requireNonNull(value);
            this.extraOptions.put(type, value);
            return this;
        }

        /**
         * Builds a new instance.
         * @param connectionPool the connection pool
         * @return the built instance
         */
        public JdbcProfile build(ConnectionPool connectionPool) {
            return new JdbcProfile(this, connectionPool);
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "fetch={0}/{1}, insert={2}/{3}, options={4}, extra={5}", //$NON-NLS-1$
                    fetchSize, maxInputConcurrency,
                    insertSize, maxOutputConcurrency,
                    optimizations, extraOptions.values());
        }
    }
}
