/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import static com.asakusafw.dag.compiler.builtin.Util.*;
import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.lang.compiler.analyzer.util.GroupOperatorUtil;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.operator.CoGroup;

/**
 * Generates {@link CoGroup} operator.
 * @since 0.4.0
 * @version 0.4.1
 */
public class CoGroupOperatorGenerator extends UserOperatorNodeGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return CoGroup.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        return gen(context, operator, namer);
    }

    static NodeInfo gen(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i >= 1, i -> i >= 1);
        return new OperatorNodeInfo(
                context.cache(CacheKey.of(operator), () -> generateClass(context, operator, namer.get())),
                Descriptions.typeOf(CoGroupOperation.Input.class),
                getDependencies(context, operator));
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return getDefaultDependencies(context, operator);
    }

    private static ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef impl = defineOperatorField(writer, operator, target);
        Map<OperatorProperty, FieldRef> map = defineConstructor(context, operator, target, writer, method -> {
            setOperatorField(method, operator, impl);
        });
        defineResultAdd(writer, method -> {
            cast(method, 1, Descriptions.typeOf(CoGroupOperation.Input.class));
            List<ValueRef> arguments = new ArrayList<>();
            arguments.add(impl);
            for (OperatorInput input : getPrimaryInputs(operator)) {
                if (isReadOnce(input)) {
                    arguments.add(v -> getGroupIterable(v, context, input));
                } else {
                    arguments.add(v -> getGroupList(v, context, input));
                }
            }
            appendSecondaryInputs(arguments::add, operator, map::get);
            appendOutputs(arguments::add, operator, map::get);
            appendArguments(arguments::add, operator, map::get);
            invoke(method, context, operator, arguments);
        });
        return new ClassData(target, writer::toByteArray);
    }

    private static boolean isReadOnce(OperatorInput port) {
        BufferType type = GroupOperatorUtil.getBufferType(port);
        return type == BufferType.VOLATILE;
    }
}
