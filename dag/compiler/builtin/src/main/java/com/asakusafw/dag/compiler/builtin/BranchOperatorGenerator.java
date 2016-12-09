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
import java.util.stream.Stream;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.analyzer.util.BranchOperatorUtil;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Branch;

/**
 * Generates {@link Branch} operator.
 * @since 0.4.0
 */
public class BranchOperatorGenerator extends UserOperatorNodeGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return Branch.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i == 1, i -> i >= 1);
        return new OperatorNodeInfo(
                context.cache(CacheKey.of(operator), () -> generateClass(context, operator, namer.get())),
                operator.getInput(0).getDataType(),
                getDependencies(context, operator));
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return getDefaultDependencies(context, operator);
    }

    private static ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        OperatorInput input = operator.getInput(0);
        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef impl = defineOperatorField(writer, operator, target);
        Map<OperatorProperty, FieldRef> map = defineConstructor(context, operator, target, writer, method -> {
            setOperatorField(method, operator, impl);
        });
        defineResultAdd(writer, method -> {
            LocalVarRef inputVar = cast(method, 1, input.getDataType());
            List<ValueRef> arguments = new ArrayList<>();
            arguments.add(impl);
            arguments.add(inputVar);
            arguments.addAll(Lang.project(operator.getArguments(), e -> map.get(e)));
            invoke(method, context, operator, arguments);
            branch(
                    method, context, operator,
                    inputVar,
                    map);
        });
        return new ClassData(target, writer::toByteArray);
    }

    static void branch(
            MethodVisitor method, Context context, UserOperator operator,
            LocalVarRef input, Map<OperatorProperty, FieldRef> dependencies) {
        OperatorOutput[] outputs = outputs(context, operator);
        Label[] caseLabels = Stream.of(outputs).map(o -> new Label()).toArray(Label[]::new);
        Label defaultLabel = new Label();
        Label endLabel = new Label();

        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                typeOf(Enum.class).getInternalName(),
                "ordinal",
                Type.getMethodDescriptor(Type.INT_TYPE),
                false);
        method.visitTableSwitchInsn(
                0, caseLabels.length - 1,
                defaultLabel, caseLabels);

        for (int i = 0; i < outputs.length; i++) {
            method.visitLabel(caseLabels[i]);
            FieldRef ref = Invariants.requireNonNull(dependencies.get(outputs[i]));
            ref.load(method);
            input.load(method);
            invokeResultAdd(method);
            method.visitJumpInsn(Opcodes.GOTO, endLabel);
        }
        method.visitLabel(defaultLabel);
        getNew(method, Descriptions.typeOf(AssertionError.class));
        method.visitInsn(Opcodes.ATHROW);

        method.visitLabel(endLabel);
    }

    private static OperatorOutput[] outputs(Context context, UserOperator operator) {
        Map<OperatorOutput, Enum<?>> branch = Invariants.safe(
                () -> BranchOperatorUtil.getOutputMap(context.getClassLoader(), operator));
        Invariants.require(branch.size() == operator.getOutputs().size());
        OperatorOutput[] results = new OperatorOutput[branch.size()];
        branch.forEach((k, v) -> {
            results[v.ordinal()] = k;
        });
        return results;
    }
}
