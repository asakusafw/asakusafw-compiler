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
package com.asakusafw.dag.compiler.builtin;

import java.util.function.Supplier;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.vocabulary.operator.Extend;

/**
 * Generates {@link Extend} operator.
 * @since 0.4.0
 */
public class ExtendOperatorGenerator extends CoreOperatorNodeGenerator {

    @Override
    protected CoreOperatorKind getOperatorKind() {
        return CoreOperatorKind.EXTEND;
    }

    @Override
    protected NodeInfo generate(Context context, CoreOperator operator, Supplier<? extends ClassDescription> namer) {
        return ProjectOperatorGenerator.gen(context, operator, namer);
    }
}
