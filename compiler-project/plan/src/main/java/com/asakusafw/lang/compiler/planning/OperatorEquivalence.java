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
package com.asakusafw.lang.compiler.planning;

import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Extracts ID of operator equivalences.
 */
public interface OperatorEquivalence {

    /**
     * Use original serial number as ID.
     */
    OperatorEquivalence SAME_ORIGIN = new OperatorEquivalence() {
        @Override
        public Object extract(SubPlan owner, Operator operator) {
            return operator.getOriginalSerialNumber();
        }
    };

    /**
     * Returns the equivalence ID from the target operator.
     * @param owner the owner of the target operator
     * @param operator the target operator
     * @return the equivalence ID
     */
    Object extract(SubPlan owner, Operator operator);
}
