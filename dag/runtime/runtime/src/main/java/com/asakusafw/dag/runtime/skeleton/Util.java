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
package com.asakusafw.dag.runtime.skeleton;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.table.HeapKeyBuffer;
import com.asakusafw.dag.runtime.table.NioKeyBuffer;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Utilities for this package.
 * @since 0.4.0
 */
final class Util {

    /**
     * The configuration key of the implementation class name of {@link KeyBuffer}.
     */
    public static final String KEY_KEY_BUFFER_TYPE = "com.asakusafw.dag.key.buffer.class"; //$NON-NLS-1$

    private static final Map<String, Supplier<? extends KeyBuffer>> BUILTIN_KEY_BUFFERS;
    static {
        Map<String, Supplier<? extends KeyBuffer>> map = new HashMap<>();
        map.put(HeapKeyBuffer.class.getName(), HeapKeyBuffer::new);
        map.put(NioKeyBuffer.class.getName(), NioKeyBuffer::new);
        BUILTIN_KEY_BUFFERS = map;
    }

    private Util() {
        return;
    }

    static int getProperty(
            ProcessorContext context,
            String title, String key, int defaultValue) {
        return context.getProperty(key)
                .map(value -> {
                    try {
                        return Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(MessageFormat.format(
                                "{0} must be a valid integer: {1}={2}",
                                title, key, value), e);
                    }
                })
                .orElse(defaultValue);
    }

    static <T extends Enum<T>> T getProperty(
            ProcessorContext context,
            String title, String key, T defaultValue) {
        return context.getProperty(key)
                .map(value -> {
                    try {
                        return Enum.valueOf(defaultValue.getDeclaringClass(), value.toUpperCase(Locale.ENGLISH));
                    } catch (NoSuchElementException e) {
                        throw new IllegalArgumentException(MessageFormat.format(
                                "{0} must be one of '{'{1}'}': {2}={3}",
                                title,
                                Arrays.stream(defaultValue.getDeclaringClass().getEnumConstants())
                                    .map(Enum::name)
                                    .collect(Collectors.joining()),
                                key, value), e);
                    }
                })
                .orElse(defaultValue);
    }

    static <T> Supplier<T> toSupplier(Class<? extends T> aClass) {
        if (aClass == null) {
            return null;
        }
        return () -> Lang.safe(() -> aClass.newInstance());
    }

    static Supplier<? extends KeyBuffer> getKeyBufferSupplier(ProcessorContext context) {
        return context.getProperty(KEY_KEY_BUFFER_TYPE)
                .<Supplier<? extends KeyBuffer>>flatMap(value -> {
                    if (BUILTIN_KEY_BUFFERS.containsKey(value)) {
                        return Optionals.of(BUILTIN_KEY_BUFFERS.get(value));
                    }
                    try {
                        Class<? extends KeyBuffer> theClass = context.getClassLoader()
                                .loadClass(value)
                                .asSubclass(KeyBuffer.class);
                        return Optionals.of(() -> Invariants.safe(theClass::newInstance));
                    } catch (ReflectiveOperationException | ClassCastException e) {
                        throw new IllegalArgumentException(MessageFormat.format(
                                "error occurred while preparing KeyBuffer supplier: {0}={1}",
                                KEY_KEY_BUFFER_TYPE, value), e);
                    }
                })
                .orElse(HeapKeyBuffer::new);
    }
}
