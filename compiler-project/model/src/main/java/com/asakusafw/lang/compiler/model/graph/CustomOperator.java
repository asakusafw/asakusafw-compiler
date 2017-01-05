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
package com.asakusafw.lang.compiler.model.graph;

/**
 * Represents a custom operator.
 * @since 0.3.0
 */
public abstract class CustomOperator extends Operator {

    /**
     * Returns the category tag of this operator.
     * @return the category tag
     */
    public abstract String getCategory();

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.CUSTOM;
    }
}
