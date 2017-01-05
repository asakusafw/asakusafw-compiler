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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil;
import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vocabulary.operator.MasterJoin;

/**
 * Generates {@link MasterJoin} operator.
 * @since 0.4.0
 */
public class MasterJoinOperatorGenerator extends MasterJoinLikeOperatorGenerator {

    private static final String FIELD_BUFFER = "buffer";

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return MasterJoin.class;
    }

    @Override
    protected Consumer<MethodVisitor> defineExtraFields(
            ClassVisitor writer, Context context,
            UserOperator operator, ClassDescription target) {
        OperatorOutput joined = operator.getOutput(MasterJoin.ID_OUTPUT_JOINED);
        FieldRef field = defineField(writer, target, FIELD_BUFFER, typeOf(joined.getDataType()));
        return method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, joined.getDataType());
            putField(method, field);
        };
    }

    @Override
    protected void defineProcess(
            MethodVisitor method,
            Context context,
            UserOperator operator,
            LocalVarRef master, LocalVarRef transaction,
            FieldRef impl,
            Map<OperatorProperty, FieldRef> dependencies,
            ClassDescription target) {
        OperatorOutput joined = operator.getOutput(MasterJoin.ID_OUTPUT_JOINED);
        OperatorOutput missed = operator.getOutput(MasterJoin.ID_OUTPUT_MISSED);

        Label onNull = new Label();
        Label end = new Label();
        master.load(method);
        getConst(method, null);
        method.visitJumpInsn(Opcodes.IF_ACMPEQ, onNull);

        dependencies.get(joined).load(method);
        performJoin(method, context, operator, master, transaction, impl, dependencies, target);
        invokeResultAdd(method);
        method.visitJumpInsn(Opcodes.GOTO, end);

        method.visitLabel(onNull);
        dependencies.get(missed).load(method);
        transaction.load(method);
        invokeResultAdd(method);

        method.visitLabel(end);
    }

    private static void performJoin(
            MethodVisitor method,
            Context context,
            UserOperator operator,
            LocalVarRef masterRef, LocalVarRef txRef,
            FieldRef impl,
            Map<OperatorProperty, FieldRef> dependencies,
            ClassDescription target) {
        OperatorInput master = operator.getInput(MasterJoin.ID_INPUT_MASTER);
        OperatorInput tx = operator.getInput(MasterJoin.ID_INPUT_TRANSACTION);
        OperatorOutput joined = operator.getOutput(MasterJoin.ID_OUTPUT_JOINED);

        method.visitVarInsn(Opcodes.ALOAD, 0);
        getField(method, target, FIELD_BUFFER, typeOf(joined.getDataType()));
        LocalVarRef bufferVar = putLocalVar(method, Type.OBJECT, 3);

        bufferVar.load(method);
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(joined.getDataType()).getInternalName(),
                "reset",
                Type.getMethodDescriptor(Type.VOID_TYPE),
                false);

        List<PropertyMapping> mappings = Invariants.safe(
                () -> JoinedModelUtil.getPropertyMappings(context.getClassLoader(), operator));
        Map<OperatorInput, ValueRef> inputs = new HashMap<>();
        Map<OperatorOutput, ValueRef> outputs = new HashMap<>();
        inputs.put(master, masterRef);
        inputs.put(tx, txRef);
        outputs.put(joined, bufferVar);
        mapping(method, context.getDataModelLoader(), mappings, inputs, outputs);
        bufferVar.load(method);
    }
}
