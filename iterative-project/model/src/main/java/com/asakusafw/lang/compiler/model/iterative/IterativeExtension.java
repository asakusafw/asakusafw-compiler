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
package com.asakusafw.lang.compiler.model.iterative;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.OperatorAttribute;

/**
 * An attribute that represents the target is an iterative element.
 * @since 0.3.0
 */
public class IterativeExtension implements OperatorAttribute {

    private final Set<String> parameters;

    /**
     * Creates a new instance.
     */
    public IterativeExtension() {
        this.parameters = Collections.emptySet();
    }

    /**
     * Creates a new instance.
     * @param parameters the iterative parameter names
     */
    public IterativeExtension(String... parameters) {
        this(Arrays.asList(Objects.requireNonNull(parameters)));
    }

    /**
     * Creates a new instance.
     * @param parameters the iterative parameter names
     */
    public IterativeExtension(Collection<String> parameters) {
        Objects.requireNonNull(parameters);
        this.parameters = Collections.unmodifiableSet(new LinkedHashSet<>(parameters));
    }

    @Override
    public OperatorAttribute copy() {
        return this;
    }

    /**
     * Returns whether the target is a scoped iterative element or not. If so, the target element will be re-evaluated
     * if any own {@link #getParameters() iterative parameters} were changed. Another, the <em>scope-less iterative
     * elements</em> will be re-evaluated for every iterations.
     * @return {@code true} if the target is a scoped iterative element, otherwise {@code false}
     */
    public boolean isScoped() {
        return parameters.isEmpty() == false;
    }

    /**
     * Returns the iterative parameter names.
     * @return the parameter names, or an empty set if the target represents a scope-less iterative element
     * @see #isScoped()
     */
    public Set<String> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Iterative{0}", //$NON-NLS-1$
                parameters);
    }
}
