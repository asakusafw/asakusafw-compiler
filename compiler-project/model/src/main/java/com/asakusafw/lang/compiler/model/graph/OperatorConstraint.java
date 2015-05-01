/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
 * An attribute of operator.
 */
public enum OperatorConstraint {

    /**
     * the operator may produce non-empty datasets even if the operator inputs are empty.
     */
    GENERATOR,

    /**
     * the operator must affect at most once per data record.
     */
    AT_MOST_ONCE,

    /**
     * the operator must affect at lease once per data record.
     * That is, this prevents 'dead code elimination'.
     */
    AT_LEAST_ONCE,
}
