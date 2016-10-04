/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.jdbc.util;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.basic.SplitJdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcContext;
import com.asakusafw.dag.runtime.jdbc.operation.OutputClearKind;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * WindGate adapter for JDBC operations.
 * @since 0.4.0
 */
public final class WindGateJdbcDirect {

    /**
     * Copy of {@code com.asakusafw.windgate.core.vocabulary.JdbcProcess.OptionSymbols.CORE_SPLIT_PREFIX}.
     */
    static final String OPTIMIAZATION_CORE_SPLIT_PREFIX = "CORE_SPLIT_BY:"; //$NON-NLS-1$

    /**
     * Copy of {@code com.asakusafw.windgate.core.vocabulary.JdbcProcess.OptionSymbols.ORACLE_DIRPATH}.
     */
    static final String OPTIMIAZATION_ORACLE_DIRPATH = "ORACLE_DIRPATH"; //$NON-NLS-1$

    private WindGateJdbcDirect() {
        return;
    }

    /**
     * Returns a new build for building WindGate JDBC input.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param adapters the JDBC adapter provider
     * @return the created builder
     */
    public static InputBuilder input(
            String profileName,
            String tableName,
            List<String> columnNames,
            Supplier<? extends ResultSetAdapter<?>> adapters) {
        return new InputBuilder(profileName, tableName, columnNames, adapters);
    }

    static Function<? super JdbcContext, ? extends JdbcInputDriver> input(InputBuilder builder) {
        Arguments.requireNonNull(builder);
        String profileName = builder.profileName;
        String tableName = builder.tableName;
        List<String> columnNames = builder.columnNames;
        String condition = builder.condition;
        Set<String> options = builder.options;
        Supplier<? extends ResultSetAdapter<?>> adapters = builder.adapters;
        return context -> {
            JdbcProfile profile = context.getEnvironment().getProfile(profileName);
            Optional<String> cond = resolve(context, condition);
            Optional<String> split = getSplit(options)
                    .filter(s -> profile.getMaxInputConcurrency().orElse(1) > 1);
            return split
                    .map(s -> buildSplitInput(profile, tableName, columnNames, cond, s, adapters, options))
                    .orElseGet(() -> buildBasicInput(profile, tableName, columnNames, cond, adapters, options));
        };
    }

    private static Optional<String> getSplit(Set<String> options) {
        List<String> columns = options.stream()
                .filter(s -> s.startsWith(OPTIMIAZATION_CORE_SPLIT_PREFIX))
                .map(s -> s.substring(OPTIMIAZATION_CORE_SPLIT_PREFIX.length()))
                .map(String::trim)
                .collect(Collectors.toList());
        if (columns.size() >= 2) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "conflict split column: {0}",
                    columns));
        }
        return columns.stream().findAny();
    }

    private static JdbcInputDriver buildBasicInput(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Optional<String> cond,
            Supplier<? extends ResultSetAdapter<?>> adapters,
            Set<String> options) {
        String query = buildSelectStatement(profile, tableName, columnNames, cond, options);
        int fetchSize = profile.getFetchSize().orElse(-1);
        return new BasicJdbcInputDriver(query, adapters, fetchSize);
    }

    private static JdbcInputDriver buildSplitInput(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Optional<String> cond,
            String splitColumn,
            Supplier<? extends ResultSetAdapter<?>> adapters,
            Set<String> options) {
        int count = profile.getMaxInputConcurrency().orElse(1);
        String condition = cond.orElse(null);
        return new SplitJdbcInputDriver(
                profile, tableName, columnNames,
                splitColumn, count,
                condition, adapters);
    }

    /**
     * Returns a new build for building WindGate JDBC output.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param adapters the JDBC adapter provider
     * @return the created builder
     */
    public static OutputBuilder output(
            String profileName,
            String tableName,
            List<String> columnNames,
            Supplier<? extends PreparedStatementAdapter<?>> adapters) {
        return new OutputBuilder(profileName, tableName, columnNames, adapters);
    }

    static Function<? super JdbcContext, ? extends JdbcOutputDriver> output(OutputBuilder builder) {
        Arguments.requireNonNull(builder);
        String profileName = builder.profileName;
        String tableName = builder.tableName;
        List<String> columnNames = builder.columnNames;
        Set<String> options = builder.options;
        Supplier<? extends PreparedStatementAdapter<?>> adapters = builder.adapters;
        return context -> {
            JdbcProfile profile = context.getEnvironment().getProfile(profileName);
            String insert = buildInsertStatement(profile, tableName, columnNames, options);
            return new BasicJdbcOutputDriver(insert, adapters);
        };
    }

    /**
     * Returns a new build for building WindGate JDBC output (truncate operation only).
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @return the created builder
     */
    public static TruncateBuilder truncate(String profileName, String tableName, List<String> columnNames) {
        return new TruncateBuilder(profileName, tableName, columnNames);
    }

    static Function<? super JdbcContext, ? extends JdbcOperationDriver> truncate(TruncateBuilder builder) {
        Arguments.requireNonNull(builder);
        String profileName = builder.profileName;
        String tableName = builder.tableName;
        List<String> columnNames = builder.columnNames;
        String customTruncate = builder.customTruncate;
        Set<String> options = builder.options;
        return context -> truncate(context, profileName, tableName, columnNames, customTruncate, options);
    }

    private static JdbcOperationDriver truncate(
            JdbcContext context,
            String profileName,
            String tableName, List<String> columnNames, String customTruncate,
            Set<String> options) {
        JdbcProfile profile = context.getEnvironment().getProfile(profileName);
        String statement = resolve(context, customTruncate)
                .orElseGet(() -> buildTruncateStatement(profile, tableName, columnNames, options));
        if (statement == null) {
            return connection -> Lang.pass();
        } else {
            return new BasicJdbcOperationDriver(statement);
        }
    }

    private static Optional<String> resolve(JdbcContext context, String pattern) {
        return Optionals.of(pattern).map(context::resolve);
    }

    private static boolean isActive(JdbcProfile profile, Set<String> options, String key) {
        return profile.getOptimizations().contains(key) && options.contains(key);
    }

    private static String buildSelectStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Optional<String> condition,
            Set<String> options) {
        return JdbcUtil.getSelectStatement(tableName, columnNames, condition.orElse(null));
    }

    private static String buildInsertStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Set<String> options) {
        boolean oraDirPath = isActive(profile, options, OPTIMIAZATION_ORACLE_DIRPATH);
        if (oraDirPath) {
            return buildOracleDirPathInsertStatement(tableName, columnNames);
        } else {
            return JdbcUtil.getInsertStatement(tableName, columnNames);
        }
    }

    private static String buildTruncateStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Set<String> options) {
        OutputClearKind kind = profile.getOption(OutputClearKind.class)
                .orElse(OutputClearKind.TRUNCATE);
        switch (kind) {
        case KEEP:
            return null;
        case DELETE:
            return JdbcUtil.getDeleteStatement(tableName, null);
        case TRUNCATE:
            return JdbcUtil.getTruncateStatement(tableName);
        default:
            throw new AssertionError(kind);
        }
    }

    private static String buildOracleDirPathInsertStatement(String tableName, List<String> columnNames) {
        StringBuilder buf = new StringBuilder();
        buf.append("INSERT "); //$NON-NLS-1$
        buf.append("/*+APPEND_VALUES*/ "); //$NON-NLS-1$
        buf.append("INTO "); //$NON-NLS-1$
        buf.append(tableName);
        buf.append(" ("); //$NON-NLS-1$
        buf.append(String.join(",", columnNames)); //$NON-NLS-1$
        buf.append(") "); //$NON-NLS-1$
        buf.append("VALUES "); //$NON-NLS-1$
        buf.append("("); //$NON-NLS-1$
        buf.append(String.join(",", placeholders(columnNames.size()))); //$NON-NLS-1$
        buf.append(")"); //$NON-NLS-1$
        return buf.toString();
    }

    private static List<String> placeholders(int count) {
        return Collections.nCopies(count, "?"); //$NON-NLS-1$
    }

    /**
     * A build for building {@link JdbcInputDriver}.
     * @since 0.4.0
     */
    public static final class InputBuilder {

        final String profileName;

        final String tableName;

        final List<String> columnNames;

        final Supplier<? extends ResultSetAdapter<?>> adapters;

        String condition;

        final Set<String> options = new LinkedHashSet<>();

        InputBuilder(
                String profileName,
                String tableName, List<String> columnNames,
                Supplier<? extends ResultSetAdapter<?>> adapters) {
            Arguments.requireNonNull(profileName);
            Arguments.requireNonNull(tableName);
            Arguments.requireNonNull(columnNames);
            Arguments.requireNonNull(adapters);
            this.profileName = profileName;
            this.tableName = tableName;
            this.columnNames = Arguments.freeze(columnNames);
            this.adapters = adapters;
        }

        /**
         * Add an option.
         * @param value the option
         * @return this
         */
        public InputBuilder withOption(String value) {
            this.options.add(value);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public InputBuilder withOptions(Collection<String> values) {
            values.forEach(this::withOption);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public InputBuilder withOptions(String... values) {
            Stream.of(values).forEach(this::withOption);
            return this;
        }

        /**
         * Sets the optional condition expression.
         * @param value the expression
         * @return this
         */
        public InputBuilder withCondition(String value) {
            this.condition = value;
            return this;
        }

        /**
         * Builds a driver provider.
         * @return the built object
         */
        public Function<? super JdbcContext, ? extends JdbcInputDriver> build() {
            return WindGateJdbcDirect.input(this);
        }

        /**
         * Builds a driver.
         * @param context the current context
         * @return the built object
         */
        public JdbcInputDriver build(JdbcContext context) {
            return build().apply(context);
        }
    }

    /**
     * A build for building {@link JdbcOutputDriver}.
     * @since 0.4.0
     */
    public static final class OutputBuilder {

        final String profileName;

        final String tableName;

        final List<String> columnNames;

        final Supplier<? extends PreparedStatementAdapter<?>> adapters;

        final Set<String> options = new LinkedHashSet<>();

        OutputBuilder(
                String profileName,
                String tableName, List<String> columnNames,
                Supplier<? extends PreparedStatementAdapter<?>> adapters) {
            Arguments.requireNonNull(profileName);
            Arguments.requireNonNull(tableName);
            Arguments.requireNonNull(columnNames);
            Arguments.requireNonNull(adapters);
            this.profileName = profileName;
            this.tableName = tableName;
            this.columnNames = Arguments.freeze(columnNames);
            this.adapters = adapters;
        }

        /**
         * Add an option.
         * @param value the option
         * @return this
         */
        public OutputBuilder withOption(String value) {
            this.options.add(value);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public OutputBuilder withOptions(Collection<String> values) {
            values.forEach(this::withOption);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public OutputBuilder withOptions(String... values) {
            Stream.of(values).forEach(this::withOption);
            return this;
        }

        /**
         * Builds a driver provider.
         * @return the built object
         */
        public Function<? super JdbcContext, ? extends JdbcOutputDriver> build() {
            return WindGateJdbcDirect.output(this);
        }

        /**
         * Builds a driver.
         * @param context the current context
         * @return the built object
         */
        public JdbcOutputDriver build(JdbcContext context) {
            return build().apply(context);
        }
    }

    /**
     * A build for building {@link JdbcOperationDriver}.
     * @since 0.4.0
     */
    public static final class TruncateBuilder {

        final String profileName;

        final String tableName;

        final List<String> columnNames;

        String customTruncate;

        final Set<String> options = new LinkedHashSet<>();

        TruncateBuilder(String profileName, String tableName, List<String> columnNames) {
            Arguments.requireNonNull(profileName);
            Arguments.requireNonNull(tableName);
            Arguments.requireNonNull(columnNames);
            this.profileName = profileName;
            this.tableName = tableName;
            this.columnNames = Arguments.freeze(columnNames);
        }

        /**
         * Add an option.
         * @param value the option
         * @return this
         */
        public TruncateBuilder withOption(String value) {
            this.options.add(value);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public TruncateBuilder withOptions(Collection<String> values) {
            values.forEach(this::withOption);
            return this;
        }

        /**
         * Adds options.
         * @param values the values
         * @return this
         */
        public TruncateBuilder withOptions(String... values) {
            Stream.of(values).forEach(this::withOption);
            return this;
        }

        /**
         * Sets the optional custom truncate statement.
         * @param value the statement
         * @return this
         */
        public TruncateBuilder withCustomTruncate(String value) {
            this.customTruncate = value;
            return this;
        }

        /**
         * Builds a driver provider.
         * @return the built object
         */
        public Function<? super JdbcContext, ? extends JdbcOperationDriver> build() {
            return WindGateJdbcDirect.truncate(this);
        }

        /**
         * Builds a driver.
         * @param context the current context
         * @return the built object
         */
        public JdbcOperationDriver build(JdbcContext context) {
            return build().apply(context);
        }
    }
}
