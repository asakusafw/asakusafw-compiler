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

import static com.asakusafw.dag.compiler.builtin.Util.*;
import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Generates {@link Update} operator.
 * @since 0.4.0
 */
public class UpdateOperatorGenerator extends UserOperatorNodeGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return Update.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i >= 1, i -> i == 1);
        return new OperatorNodeInfo(
                context.cache(CacheKey.of(operator), () -> generateClass(context, operator, namer.get())),
                operator.getInput(Update.ID_INPUT).getDataType(),
                getDependencies(context, operator));
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return getDefaultDependencies(context, operator);
    }

    private static ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        OperatorInput input = operator.getInput(Update.ID_INPUT);
        OperatorOutput output = operator.getOutput(Update.ID_OUTPUT);

        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef impl = defineOperatorField(writer, operator, target);
        Map<OperatorProperty, FieldRef> map = defineConstructor(context, operator, target, writer, method -> {
            setOperatorField(method, operator, impl);
        });
        defineResultAdd(writer, method -> {
            cast(method, 1, input.getDataType());
            List<ValueRef> arguments = new ArrayList<>();
            arguments.add(impl);
            arguments.add(new LocalVarRef(Opcodes.ALOAD, 1));
            appendSecondaryInputs(arguments::add, operator, map::get);
            appendArguments(arguments::add, operator, map::get);
            invoke(method, context, operator, arguments);

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, map.get(output));
            method.visitVarInsn(Opcodes.ALOAD, 1);
            invokeResultAdd(method);
        });
        return new ClassData(target, writer::toByteArray);
    }
}
