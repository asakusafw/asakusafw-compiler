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
package com.asakusafw.dag.compiler.builtin;

import java.lang.annotation.Annotation;
import java.util.function.Supplier;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.GroupSort;

/**
 * Generates {@link GroupSort} operator.
 * @since 0.4.0
 */
public class GroupSortOperatorGenerator extends UserOperatorNodeGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return GroupSort.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        return CoGroupOperatorGenerator.gen(context, operator, namer);
    }
}
