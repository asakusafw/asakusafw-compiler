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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;

/**
 * Composition of {@link OperatorRewriter}s.
 */
public final class CompositeOperatorRewriter implements OperatorRewriter {

    private final List<OperatorRewriter> elements;

    /**
     * Creates a new instance.
     * @param elements the element rewriters
     * @see CompositeOperatorRewriter#from(Collection)
     */
    CompositeOperatorRewriter(Collection<? extends OperatorRewriter> elements) {
        this.elements = new ArrayList<>(elements);
    }

    /**
     * Creates a new builder for this class.
     * @return the created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new instance.
     * @param elements the element rewriters
     * @return the created instance
     */
    public static OperatorRewriter from(Collection<? extends OperatorRewriter> elements) {
        if (elements.isEmpty()) {
            return OperatorRewriter.NULL;
        } else if (elements.size() == 1) {
            return elements.iterator().next();
        } else {
            List<OperatorRewriter> expanded = new ArrayList<>();
            for (OperatorRewriter element : elements) {
                if (element == OperatorRewriter.NULL) {
                    continue;
                } else if (element instanceof CompositeOperatorRewriter) {
                    expanded.addAll(((CompositeOperatorRewriter) element).getElements());
                } else {
                    expanded.add(element);
                }
            }
            return new CompositeOperatorRewriter(expanded);
        }
    }

    /**
     * Returns the element rewriters.
     * @return the element rewriters
     */
    public List<OperatorRewriter> getElements() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public void perform(Context context, OperatorGraph graph) {
        for (OperatorRewriter element : elements) {
            element.perform(context, graph);
        }
    }

    /**
     * Builder for {@link CompositeOperatorRewriter}.
     */
    public static class Builder {

        private final List<OperatorRewriter> rewriters = new ArrayList<>();

        /**
         * Sets elements from via SPI.
         * @param loader the service loader
         * @param type the service type
         * @return this
         */
        public Builder load(ClassLoader loader, Class<? extends OperatorRewriter> type) {
            return add(Util.load(loader, type));
        }

        /**
         * Adds an element.
         * @param element the element
         * @return this
         */
        public Builder add(OperatorRewriter element) {
            if (element != OperatorRewriter.NULL) {
                rewriters.add(element);
            }
            return this;
        }

        /**
         * Adds an element.
         * @param elements the elements
         * @return this
         */
        public Builder add(OperatorRewriter... elements) {
            return add(Arrays.asList(elements));
        }

        /**
         * Adds an element.
         * @param elements the elements
         * @return this
         */
        public Builder add(Collection<? extends OperatorRewriter> elements) {
            for (OperatorRewriter element : elements) {
                add(element);
            }
            return this;
        }

        /**
         * Builds a {@link OperatorRewriter}.
         * @return the built object
         */
        public OperatorRewriter build() {
            return CompositeOperatorRewriter.from(rewriters);
        }
    }
}
