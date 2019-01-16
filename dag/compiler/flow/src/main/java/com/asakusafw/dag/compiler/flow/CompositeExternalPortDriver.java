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
package com.asakusafw.dag.compiler.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A composition of {@link ExternalPortDriver}.
 * @since 0.4.0
 */
public class CompositeExternalPortDriver implements ExternalPortDriver {

    private final List<ExternalPortDriver> elements;

    /**
     * Creates a new instance.
     * @param elements the element drivers
     */
    public CompositeExternalPortDriver(Collection<? extends ExternalPortDriver> elements) {
        Arguments.requireNonNull(elements);
        this.elements = Arguments.freezeToList(elements);
    }

    /**
     * Returns {@link ExternalPortDriver} via SPI.
     * @param context the current context
     * @return the loaded {@link ExternalPortDriver}, may be an instance of {@link CompositeExternalPortDriver}
     */
    public static ExternalPortDriver load(ExternalPortDriverProvider.Context context) {
        List<ExternalPortDriver> elements = new ArrayList<>();
        ClassLoader classLoader = context.getGeneratorContext().getClassLoader();
        for (ExternalPortDriverProvider provider : ServiceLoader.load(ExternalPortDriverProvider.class, classLoader)) {
            elements.add(provider.newInstance(context));
        }
        return new CompositeExternalPortDriver(elements);
    }

    @Override
    public boolean accepts(ExternalInput port) {
        return elements.stream().anyMatch(e -> e.accepts(port));
    }

    @Override
    public boolean accepts(ExternalOutput port) {
        return elements.stream().anyMatch(e -> e.accepts(port));
    }

    @Override
    public ClassDescription processInput(ExternalInput port) {
        return elements.stream()
                .filter(e -> e.accepts(port))
                .findFirst()
                .map(e -> e.processInput(port))
                .orElseThrow(IllegalStateException::new);
    }

    @Override
    public void processOutputs(GraphInfoBuilder target) {
        elements.forEach(e -> e.processOutputs(target));
    }

    @Override
    public void processPlan(GraphInfoBuilder target) {
        elements.forEach(e -> e.processPlan(target));
    }
}
