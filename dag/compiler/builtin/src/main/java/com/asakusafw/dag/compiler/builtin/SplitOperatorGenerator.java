/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil;
import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Split;

/**
 * Generates {@link Split} operator.
 * @since 0.4.0
 */
public class SplitOperatorGenerator extends UserOperatorNodeGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return Split.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i == 1, i -> i == 2);
        checkArgs(operator, i -> i == 0);
        return new OperatorNodeInfo(
                context.cache(CacheKey.of(operator), () -> generateClass(context, operator, namer.get())),
                operator.getInput(Split.ID_INPUT).getDataType(),
                getDependencies(context, operator));
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return context.getDependencies(operator.getOutputs());
    }

    private static ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        OperatorInput input = operator.getInput(Split.ID_INPUT);
        OperatorOutput left = operator.getOutput(Split.ID_OUTPUT_LEFT);
        OperatorOutput right = operator.getOutput(Split.ID_OUTPUT_RIGHT);

        List<PropertyMapping> mappings = Invariants.safe(
                () -> JoinedModelUtil.getPropertyMappings(context.getClassLoader(), operator));

        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef leftField = defineField(writer, target, "leftBuffer", typeOf(left.getDataType()));
        FieldRef rightField = defineField(writer, target, "rightBuffer", typeOf(right.getDataType()));

        Map<OperatorOutput, FieldRef> deps = defineDependenciesConstructor(
                context, operator.getOutputs(), target, writer, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, left.getDataType());
            putField(method, leftField);
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, right.getDataType());
            putField(method, rightField);
        });

        defineResultAdd(writer, method -> {
            LocalVarRef inputVar = cast(method, 1, input.getDataType());

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, leftField);
            LocalVarRef leftVar = putLocalVar(method, Type.OBJECT, 2);

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, rightField);
            LocalVarRef rightVar = putLocalVar(method, Type.OBJECT, 3);

            Map<OperatorInput, ValueRef> inputs = new HashMap<>();
            Map<OperatorOutput, ValueRef> outputs = new HashMap<>();
            inputs.put(input, inputVar);
            outputs.put(left, leftVar);
            outputs.put(right, rightVar);
            mapping(method, context.getDataModelLoader(), mappings, inputs, outputs);

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, deps.get(left));
            leftVar.load(method);
            invokeResultAdd(method);

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, deps.get(right));
            rightVar.load(method);
            invokeResultAdd(method);
        });
        return new ClassData(target, writer::toByteArray);
    }
}
