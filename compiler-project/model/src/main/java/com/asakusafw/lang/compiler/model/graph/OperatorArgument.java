/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * An argument on operators.
 */
public class OperatorArgument implements OperatorProperty {

    private final String name;

    private final ValueDescription value;

    /**
     * Creates a new instance.
     * @param name the argument name
     * @param value the argument value
     */
    public OperatorArgument(String name, ValueDescription value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public PropertyKind getPropertyKind() {
        return PropertyKind.ARGUMENT;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the value of this argument.
     * @return the value
     */
    public ValueDescription getValue() {
        return value;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Argument(name={0}, value={1})", //$NON-NLS-1$
                name,
                value);
    }
}
