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
package com.asakusafw.dag.api.processor.basic;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.EdgeWriter;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * An abstract implementation of {@link EdgeIoProcessorContext}.
 * @param <S> the self type
 * @since 0.4.0
 */
public abstract class AbstractEdgeIoProcessorContext<S extends AbstractEdgeIoProcessorContext<S>>
        extends AbstractProcessorContext<S>
        implements EdgeIoProcessorContext {

    private final Map<String, Supplier<? extends EdgeReader>> inputs = new LinkedHashMap<>();

    private final Map<String, Supplier<? extends EdgeWriter>> outputs = new LinkedHashMap<>();

    @Override
    public EdgeReader getInput(String name) throws IOException, InterruptedException {
        return getPort(inputs, name);
    }

    @Override
    public EdgeWriter getOutput(String name) throws IOException, InterruptedException {
        return getPort(outputs, name);
    }

    private static <T> T getPort(Map<String, Supplier<? extends T>> ports, String name) {
        return Optionals.get(ports, name)
                .map(s -> s.get()) // Note: JDK cannot recognize Supplier::get
                .orElseThrow(() -> new NoSuchElementException(name));
    }

    /**
     * Adds an input reader.
     * @param name the port name
     * @param input the input reader
     * @return this
     */
    public final S withInput(String name, Supplier<? extends EdgeReader> input) {
        return withPort(inputs, name, input);
    }

    /**
     * Adds an output writer.
     * @param name the port name
     * @param output the output writer
     * @return this
     */
    public final S withOutput(String name, Supplier<? extends EdgeWriter> output) {
        return withPort(outputs, name, output);
    }

    /**
     * Adds an output writer.
     * @param name the port name
     * @param consumer the output consumer
     * @return this
     */
    public final S withOutput(String name, Consumer<Object> consumer) {
        return withPort(outputs, name, () -> (ObjectWriter) o -> consumer.accept(o));
    }

    private <T> S withPort(Map<String, T> ports, String name, T port) {
        if (ports.putIfAbsent(name, port) != null) {
            throw new IllegalStateException(name);
        }
        return self();
    }
}
