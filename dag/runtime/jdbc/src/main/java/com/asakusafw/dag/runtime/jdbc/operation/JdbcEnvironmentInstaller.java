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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.ProcessorContext.Editor;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.basic.BasicConnectionPool;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Installs {@link JdbcEnvironment} into the core processor environment.
 * @since 0.4.0
 */
public class JdbcEnvironmentInstaller implements ProcessorContextExtension {

    /**
     * The property key prefix.
     * Each property key must be in form of {@code [prefix].[profile-name].[sub-key]}.
     */
    public static final String KEY_PREFIX = "com.asakusafw.dag.jdbc."; //$NON-NLS-1$

    /**
     * The property sub-key of connection URL.
     */
    public static final String KEY_URL = "url"; //$NON-NLS-1$

    /**
     * The property sub-key of FQN of JDBC Driver.
     */
    public static final String KEY_DRIVER = "driver"; //$NON-NLS-1$

    /**
     * The property sub-key of JDBC connection properties.
     */
    public static final String KEY_PROPERTIES = "properties"; //$NON-NLS-1$

    /**
     * The property sub-key of the connection pool implementation class name.
     */
    public static final String KEY_POOL_CLASS = "connection.pool"; //$NON-NLS-1$

    /**
     * The property sub-key of the max number of connections.
     */
    public static final String KEY_POOL_SIZE = "connection.max"; //$NON-NLS-1$

    /**
     * The property sub-key of {@link ResultSet#getFetchSize() fetch size}.
     */
    public static final String KEY_FETCH_SIZE = "input.records"; //$NON-NLS-1$

    /**
     * The property sub-key of the number of threads per input.
     */
    public static final String KEY_INPUT_THREADS = "input.threads"; //$NON-NLS-1$

    /**
     * The property sub-key of {@link PreparedStatement#executeBatch() the number of batch insert records} per commit.
     */
    public static final String KEY_BATCH_INSERT_SIZE = "output.records"; //$NON-NLS-1$

    /**
     * The property sub-key of the number of threads per output.
     */
    public static final String KEY_OUTPUT_THREADS = "output.threads"; //$NON-NLS-1$

    /**
     * The property sub-key of the operation kind of clearing outputs.
     */
    public static final String KEY_OUTPUT_CLEAR = "output.clear"; //$NON-NLS-1$

    /**
     * The property sub-key of comma separated available optimization symbols.
     */
    public static final String KEY_OPTIMIZATIONS = "optimizations"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_POOL_SIZE}.
     */
    public static final int DEFAULT_POOL_SIZE = 1;

    /**
     * The default value of {@link #KEY_FETCH_SIZE}.
     */
    public static final int DEFAULT_FETCH_SIZE = 1024;

    /**
     * The default value of {@link #KEY_BATCH_INSERT_SIZE}.
     */
    public static final int DEFAULT_BATCH_INSERT_SIZE = 1024;

    /**
     * The default value of {@link #KEY_INPUT_THREADS}.
     */
    public static final int DEFAULT_INPUT_THREADS = 1;

    /**
     * The default value of {@link #KEY_OUTPUT_THREADS}.
     */
    public static final int DEFAULT_OUTPUT_THREADS = 1;

    private static final Pattern PATTERN_KEY = Pattern.compile(Pattern.quote(KEY_PREFIX) + "(\\w+)\\.(.+)"); //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(JdbcEnvironmentInstaller.class);

    @Override
    public InterruptibleIo install(ProcessorContext context, Editor editor) throws IOException, InterruptedException {
        try (Closer closer = new Closer()) {
            List<JdbcProfile> profiles = collect(context, closer);
            editor.addResource(JdbcEnvironment.class, new JdbcEnvironment(profiles));
            return closer.move();
        }
    }

    private static List<JdbcProfile> collect(ProcessorContext context, Closer closer) {
        Map<String, Map<String, String>> properties = getProfiles(context.getPropertyMap());
        List<JdbcProfile> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : properties.entrySet()) {
            String profileName = entry.getKey();
            LOG.debug("loading JDBC profile: {}", profileName); //$NON-NLS-1$
            Map<String, String> subProperties = entry.getValue();
            results.add(resolve(context, profileName, subProperties, closer));
        }
        return results;
    }

    private static JdbcProfile resolve(
            ProcessorContext context,
            String profileName, Map<String, String> properties,
            Closer closer) {
        Optionals.remove(properties, KEY_DRIVER).ifPresent(name -> {
            try {
                Class.forName(name, true, context.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "failed to load JDBC driver class: {1} ({0})",
                        qualified(profileName, KEY_DRIVER),
                        name), e);
            }
        });
        String url = extract(profileName, properties, KEY_URL);
        ConnectionPool.Provider provider = extractProvider(context, profileName, properties, KEY_POOL_CLASS);
        int maxConnections = extract(profileName, properties, KEY_POOL_SIZE, DEFAULT_POOL_SIZE);
        Map<String, String> connectionProps = extractMap(profileName, properties, KEY_PROPERTIES);

        JdbcProfile.Builder builder = new JdbcProfile.Builder(profileName)
                .withFetchSize(extract(profileName, properties, KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE))
                .withInsertSize(extract(profileName, properties, KEY_BATCH_INSERT_SIZE, DEFAULT_BATCH_INSERT_SIZE))
                .withMaxInputConcurrency(extract(profileName, properties, KEY_INPUT_THREADS, DEFAULT_INPUT_THREADS))
                .withMaxOutputConcurrency(extract(profileName, properties, KEY_OUTPUT_THREADS, DEFAULT_OUTPUT_THREADS))
                .withOptions(extractSet(profileName, properties, KEY_OPTIMIZATIONS));
        extract(OutputClearKind.class, profileName, properties, KEY_OUTPUT_CLEAR)
            .ifPresent(builder::withOption);
        if (properties.isEmpty() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "unrecognized JDBC profile properties: {0}",
                    properties.keySet().stream()
                        .map(k -> qualified(profileName, k))
                        .collect(Collectors.joining(", ", "{", "}")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("JDBC profile: name={}, jdbc={}@{}/{}, conf={{}}", new Object[] { //$NON-NLS-1$
                    profileName,
                    url, provider.getClass().getName(), maxConnections,
                    builder,
            });
        }
        return builder.build(closer.add(provider.newInstance(url, connectionProps, maxConnections)));
    }

    private static ConnectionPool.Provider extractProvider(
            ProcessorContext context, String profileName, Map<String, String> properties, String key) {
        String value = properties.remove(key);
        if (value == null) {
            return new BasicConnectionPool.Provider();
        }
        try {
            Class<?> aClass = Class.forName(value, false, context.getClassLoader());
            if (ConnectionPool.Provider.class.isAssignableFrom(aClass)) {
                return (ConnectionPool.Provider) aClass.newInstance();
            }
            for (Class<?> inner : aClass.getDeclaredClasses()) {
                if (ConnectionPool.Provider.class.isAssignableFrom(inner)
                        && inner.isInterface() == false
                        && Modifier.isPublic(inner.getModifiers())
                        && Modifier.isStatic(inner.getModifiers())
                        && Modifier.isAbstract(inner.getModifiers()) == false
                        && inner.isSynthetic() == false) {
                    return (ConnectionPool.Provider) inner.newInstance();
                }
            }
            throw new IllegalArgumentException(MessageFormat.format(
                    "failed to resolve connection pool class: {1} ({0})",
                    qualified(profileName, key),
                    value));
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "failed to resolve connection pool class: {1} ({0})",
                    qualified(profileName, key),
                    value), e);
        }
    }

    private static Map<String, Map<String, String>> getProfiles(Map<String, String> flat) {
        return flat.entrySet().stream()
                .map(Tuple::of)
                .filter(e -> e.left() != null)
                .flatMap(t -> {
                    Matcher matcher = PATTERN_KEY.matcher(t.left());
                    if (matcher.matches()) {
                        String profile = matcher.group(1);
                        String subKey = matcher.group(2);
                        return Stream.of(new Tuple<>(profile, new Tuple<>(subKey, t.right())));
                    } else {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.groupingBy(
                        Tuple::left,
                        Collectors.mapping(Tuple::right, Collectors.toMap(Tuple::left, Tuple::right))));
    }

    private static String extract(String profile, Map<String, String> properties, String key) {
        return Optionals.remove(properties, key)
                .orElseThrow(() -> new IllegalArgumentException(MessageFormat.format(
                        "missing mandatory property: {0}",
                        qualified(profile, key))));
    }

    private static int extract(String profile, Map<String, String> properties, String key, int defaultValue) {
        return Optionals.remove(properties, key)
                .map(v -> {
                    try {
                        return Integer.parseInt(v);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(MessageFormat.format(
                                "\"{0}\" must be a valid integer: {1}",
                                qualified(profile, key), v), e);
                    }
                })
                .orElse(defaultValue);
    }

    private static Set<String> extractSet(String profile, Map<String, String> properties, String key) {
        return Optionals.remove(properties, key)
                .map(s -> Stream.of(s.split(",")) //$NON-NLS-1$
                        .map(String::trim)
                        .filter(e -> e.isEmpty() == false)
                        .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
    }

    private static Map<String, String> extractMap(String profile, Map<String, String> properties, String key) {
        Pattern pattern = Pattern.compile(Pattern.quote(key) + "\\.(.+)"); //$NON-NLS-1$
        Map<String, String> results = properties.entrySet().stream()
                .map(Tuple::of)
                .flatMap(t -> {
                    Matcher matcher = pattern.matcher(t.left());
                    if (matcher.matches()) {
                        String subKey = matcher.group(1);
                        return Stream.of(new Tuple<>(subKey, t.right()));
                    } else {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(Tuple::left, Tuple::right));
        for (Iterator<String> iter = properties.keySet().iterator(); iter.hasNext();) {
            String next = iter.next();
            if (next != null && pattern.matcher(next).matches()) {
                iter.remove();
            }
        }
        return results;
    }

    private static <E extends Enum<E>> Optional<E> extract(
            Class<E> enumType,
            String profile, Map<String, String> properties, String key) {
        return Optionals.remove(properties, key)
                .map(String::trim)
                .filter(s -> s.isEmpty() == false)
                .map(s -> {
                    try {
                        return Enum.valueOf(enumType, s.toUpperCase(Locale.ENGLISH));
                    } catch (NoSuchElementException e) {
                        throw new IllegalArgumentException(MessageFormat.format(
                                "unknown name: {1} ({0}) must be one of {2}",
                                qualified(profile, key),
                                s.toUpperCase(Locale.ENGLISH),
                                Stream.of(enumType.getEnumConstants())
                                    .map(Enum::name)
                                    .collect(Collectors.joining(", "))), e); //$NON-NLS-1$
                    }
                });
    }

    static String qualified(String profile, String key) {
        return String.format("%s%s.%s", KEY_PREFIX, profile, key); //$NON-NLS-1$
    }
}
