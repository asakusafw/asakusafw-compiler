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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Composition of {@link ContextHandler}.
 * @param <T> the acceptable context type
 * @since 0.4.0
 */
public final class CompositeContextHandler<T extends ProcessorContext> implements ContextHandler<T> {

    private final List<ContextHandler<? super T>> elements;

    private CompositeContextHandler(List<? extends ContextHandler<? super T>> elements) {
        Arguments.requireNonNull(elements);
        this.elements = new ArrayList<>(elements);
    }

    /**
     * Creates a composite context handler with the specified element handlers.
     * @param <T> the context type
     * @param elements the element handlers
     * @return the composite context handler
     */
    public static <T extends ProcessorContext> ContextHandler<T> of(
            List<? extends ContextHandler<? super T>> elements) {
        Arguments.requireNonNull(elements);
        if (elements.isEmpty()) {
            return new NullContextHandler<>();
        } else if (elements.size() == 1) {
            return new SafeContextHandler<>(elements.get(0));
        } else {
            return new CompositeContextHandler<>(elements);
        }
    }

    /**
     * Creates a composite context handler with the specified element handlers.
     * @param <T> the context type
     * @param elements the element handlers
     * @return the composite context handler
     */
    @SafeVarargs
    public static <T extends ProcessorContext> ContextHandler<T> of(
            ContextHandler<? super T>... elements) {
        Arguments.requireNonNull(elements);
        return of(Arrays.asList(elements));
    }

    /**
     * Creates a composite context handler with the specified element handlers.
     * @param <T> the context type
     * @param element the element handler
     * @return the composite context handler
     */
    public static <T extends ProcessorContext> ContextHandler<T> of(
            Optional<? extends ContextHandler<? super T>> element) {
        Arguments.requireNonNull(element);
        return element.map(h -> (ContextHandler<T>) new SafeContextHandler<>(h)).orElseGet(NullContextHandler::new);
    }

    @Override
    public Session start(T context) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        Session[] ss = new Session[elements.size()];
        boolean success = false;
        int index = 0;
        try {
            for (ContextHandler<? super T> element : elements) {
                ss[index] = element.start(context);
                index++;
            }
            success = true;
        } finally {
            if (success == false) {
                new CompositeSession(ss).close();
            }
        }
        return new CompositeSession(ss);
    }

    private static final class CompositeSession implements Session {

        private final Session[] elements;

        CompositeSession(Session[] elements) {
            this.elements = elements;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            Session[] ss = elements;
            for (int i = ss.length - 1; i >= 0; i--) {
                if (ss[i] != null) {
                    ss[i].close();
                    ss[i] = null;
                }
            }
        }
    }

    private static class NullContextHandler<T extends ProcessorContext> implements ContextHandler<T> {

        private static final Session NULL = new Session() {
            @Override
            public void close() {
                return;
            }
        };

        NullContextHandler() {
            return;
        }

        @Override
        public Session start(T context) {
            return NULL;
        }
    }

    private static class SafeContextHandler<T extends ProcessorContext> implements ContextHandler<T> {

        private final ContextHandler<? super T> element;

        SafeContextHandler(ContextHandler<? super T> element) {
            this.element = element;
        }

        @Override
        public Session start(T context) throws IOException, InterruptedException {
            return element.start(context);
        }
    }
}
