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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil;
import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.codegen.ObjectCopierGenerator;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.ValueElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement.ElementKind;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.skeleton.CombineResult;
import com.asakusafw.dag.runtime.skeleton.SimpleCombineResult;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.vocabulary.operator.Fold;

/**
 * Generates {@link Fold} operator.
 * @since 0.4.0
 */
public class FoldOperatorGenerator extends UserOperatorNodeGenerator {

    static final String SUFFIX_COMBINER = "$Combiner";

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return Fold.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        if (getSecondaryInputs(operator).isEmpty()) {
            checkPorts(operator, i -> i == 1, i -> i == 1);
            CacheKey key = CacheKey.builder()
                    .operator(operator)
                    .arguments(operator) // aggregate operation embeds its arguments into combiner class
                    .build();
            ClassData adapter = context.cache(key, () -> generateClass(context, operator, namer.get()));
            return new AggregateNodeInfo(
                    adapter,
                    null,
                    ObjectCopierGenerator.get(context, operator.getInput(Fold.ID_INPUT).getDataType()),
                    getCombinerName(adapter.getDescription()),
                    operator.getInput(Fold.ID_INPUT).getDataType(),
                    operator.getOutput(Fold.ID_OUTPUT).getDataType(),
                    getDependencies(context, operator));
        } else {
            // If the operator has data tables, we generate simplified operator implementation.
            // The simplified implementation cannot perform pre-aggregations.
            return new OperatorNodeInfo(
                    context.cache(CacheKey.of(operator), () -> generateSimpleClass(context, operator, namer.get())),
                    Descriptions.typeOf(CoGroupOperation.Input.class),
                    getSimpleDependencies(context, operator));
        }
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return context.getDependencies(operator.getOutputs());
    }

    private static ClassDescription getCombinerName(ClassDescription outer) {
        return new ClassDescription(outer.getBinaryName() + SUFFIX_COMBINER);
    }

    private ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        ClassDescription combinerClass = generateCombinerClass(context, operator, target);
        ClassWriter writer = newWriter(target, CombineResult.class);
        writer.visitInnerClass(
                combinerClass.getInternalName(),
                target.getInternalName(),
                combinerClass.getSimpleName(),
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC);

        OperatorInput input = operator.getInput(Fold.ID_INPUT);
        defineDependenciesConstructor(context, operator.getOutputs(), target, writer, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, combinerClass);
            getNew(method, input.getDataType());
            method.visitVarInsn(Opcodes.ALOAD, 1);
            method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    typeOf(CombineResult.class).getInternalName(),
                    "<init>",
                    Type.getMethodDescriptor(Type.VOID_TYPE,
                            typeOf(ObjectCombiner.class), typeOf(DataModel.class), typeOf(Result.class)),
                    false);
        }, Lang.discard());
        writer.visitEnd();

        return new ClassData(target, writer::toByteArray);
    }

    private ClassDescription generateCombinerClass(Context context, UserOperator operator, ClassDescription outer) {
        ClassDescription target = getCombinerName(outer);
        OperatorInput input = operator.getInput(0);

        ClassWriter writer = newWriter(target, Object.class, ObjectCombiner.class);
        writer.visitOuterClass(outer.getInternalName(), target.getInternalName(), null);

        FieldRef impl = defineOperatorField(writer, operator, target);
        defineEmptyConstructor(writer, Object.class, method -> {
            setOperatorField(method, operator, impl);
        });
        defineBuildKey(context, writer, input.getDataType(), input.getGroup());

        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "combine",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class), typeOf(Object.class)),
                null,
                null);

        List<ValueRef> arguments = new ArrayList<>();
        arguments.add(impl);
        arguments.add(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 1);
            m.visitTypeInsn(Opcodes.CHECKCAST, typeOf(input.getDataType()).getInternalName());
        });
        arguments.add(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 2);
            m.visitTypeInsn(Opcodes.CHECKCAST, typeOf(input.getDataType()).getInternalName());
        });
        for (VertexElement dep : context.getDependencies(operator.getArguments())) {
            Invariants.require(dep.getElementKind() == ElementKind.VALUE);
            ValueDescription value = ((ValueElement) dep).getValue();
            arguments.add(m -> {
                getConst(method, Invariants.safe(() -> value.resolve(context.getClassLoader())));
            });
        }
        invoke(method, context, operator, arguments);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
        return context.addClassFile(new ClassData(target, writer::toByteArray));
    }

    private static List<VertexElement> getSimpleDependencies(Context context, UserOperator operator) {
        return getDefaultDependencies(context, operator);
    }

    private ClassData generateSimpleClass(Context context, UserOperator operator, ClassDescription target) {
        TypeDescription dataType = operator.getInput(Fold.ID_INPUT).getDataType();

        ClassWriter writer = newWriter(target, SimpleCombineResult.class);
        FieldRef impl = defineOperatorField(writer, operator, target);
        FieldRef acc = defineField(writer, target, "acc", typeOf(dataType)); //$NON-NLS-1$
        Map<OperatorProperty, FieldRef> map = defineConstructor(context, operator, target, writer,
                method -> {
                    method.visitVarInsn(Opcodes.ALOAD, 0);
                    method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                            AsmUtil.typeOf(SimpleCombineResult.class).getInternalName(),
                            CONSTRUCTOR_NAME,
                            Type.getMethodDescriptor(Type.VOID_TYPE),
                            false);

                    setOperatorField(method, operator, impl);

                    method.visitVarInsn(Opcodes.ALOAD, 0);
                    getNew(method, dataType);
                    putField(method, acc);
                },
                method -> Lang.pass());
        defineSimpleStart(operator, writer, acc);
        defineSimpleCombine(context, operator, writer, impl, acc, map);
        defineSimpleFinish(writer, acc, map.get(operator.getOutput(Fold.ID_OUTPUT)));
        return new ClassData(target, writer::toByteArray);
    }

    private void defineSimpleStart(UserOperator operator, ClassWriter writer, FieldRef acc) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "start",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class)),
                null,
                null);

        TypeDescription dataType = operator.getInput(Fold.ID_INPUT).getDataType();

        acc.load(method);
        method.visitVarInsn(Opcodes.ALOAD, 1);
        method.visitTypeInsn(Opcodes.CHECKCAST, typeOf(dataType).getInternalName());
        copyDataModel(method, dataType);

        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    private void defineSimpleCombine(
            Context context, UserOperator operator, ClassWriter writer, FieldRef impl, FieldRef acc,
            Map<OperatorProperty, FieldRef> map) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "combine",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class)),
                null,
                null);

        TypeDescription dataType = operator.getInput(Fold.ID_INPUT).getDataType();
        LocalVarRef object = cast(method, 1, dataType);

        List<ValueRef> arguments = new ArrayList<>();
        arguments.add(impl);
        arguments.add(acc);
        arguments.add(object);
        appendSecondaryInputs(arguments::add, operator, map::get);
        appendArguments(arguments::add, operator, map::get);
        invoke(method, context, operator, arguments);

        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    private void defineSimpleFinish(ClassWriter writer, FieldRef acc, ValueRef result) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "finish",
                Type.getMethodDescriptor(typeOf(void.class)),
                null,
                null);

        result.load(method);
        acc.load(method);
        invokeResultAdd(method);

        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }
}
