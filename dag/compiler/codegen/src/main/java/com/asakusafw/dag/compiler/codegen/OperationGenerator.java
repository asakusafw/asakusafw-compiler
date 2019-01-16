/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.ClassNode;
import com.asakusafw.dag.compiler.model.graph.DataTableNode;
import com.asakusafw.dag.compiler.model.graph.OperationSpec;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.ValueElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement.ElementKind;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.runtime.core.Result;

/**
 * Generates {@link Operation} classes.
 * @since 0.4.0
 */
public class OperationGenerator {

    private static final String FIELD_CONTEXT = "context"; //$NON-NLS-1$

    /**
     * Generates operation graph generator.
     * @param context the current context
     * @param graph the target graph
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, OperationSpec graph, ClassDescription target) {
        ClassWriter writer = newWriter(target, Object.class, Operation.class);
        List<VertexElement> elements = graph.getSorted().stream()
                .filter(e -> e.getElementKind() != ElementKind.INPUT)
                .collect(Collectors.toList());

        addContextField(writer);
        addElementFields(writer, target, elements, graph::getId);
        addElementMethods(writer, target, elements, graph::getId);
        addConstructor(writer, target, elements, graph::getId);
        addProcessMethod(writer, target, graph);
        return new ClassData(target, writer::toByteArray);
    }

    private static void addConstructor(ClassWriter writer, ClassDescription target,
            List<VertexElement> elements, Function<VertexElement, String> ids) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(OperationAdapter.Context.class)),
                null,
                null);
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                typeOf(Object.class).getInternalName(),
                "<init>",
                Type.getMethodDescriptor(Type.VOID_TYPE),
                false);

        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitVarInsn(Opcodes.ALOAD, 1);
        method.visitFieldInsn(
                Opcodes.PUTFIELD,
                target.getInternalName(),
                FIELD_CONTEXT,
                typeOf(OperationAdapter.Context.class).getDescriptor());

        for (VertexElement element : elements) {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            method.visitMethodInsn(
                    Opcodes.INVOKESPECIAL,
                    target.getInternalName(),
                    ids.apply(element),
                    Type.getMethodDescriptor(Type.VOID_TYPE),
                    false);
        }
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    private static void addProcessMethod(ClassWriter writer, ClassDescription target, OperationSpec graph) {
        VertexElement consumer = graph.getInput().getConsumer();
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "process",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class)),
                null,
                null);
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitFieldInsn(Opcodes.GETFIELD,
                target.getInternalName(),
                graph.getId(consumer),
                typeOf(consumer.getRuntimeType()).getDescriptor());
        method.visitVarInsn(Opcodes.ALOAD, 1);
        invokeResultAdd(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    private static void addContextField(ClassWriter writer) {
        writer.visitField(
                Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
                FIELD_CONTEXT,
                typeOf(OperationAdapter.Context.class).getDescriptor(),
                null,
                null);
    }

    private static void addElementFields(ClassWriter writer, ClassDescription target,
            List<VertexElement> elements, Function<VertexElement, String> ids) {
        for (VertexElement element : elements) {
            writer.visitField(
                    Opcodes.ACC_PRIVATE,
                    ids.apply(element),
                    typeOf(element.getRuntimeType()).getDescriptor(),
                    null,
                    null);
        }
    }

    private static void addElementMethods(
            ClassWriter writer, ClassDescription target,
            List<VertexElement> elements, Function<VertexElement, String> ids) {
        for (VertexElement element : elements) {
            String id = ids.apply(element);
            MethodVisitor method = writer.visitMethod(
                    Opcodes.ACC_PRIVATE,
                    id,
                    Type.getMethodDescriptor(Type.VOID_TYPE),
                    null,
                    null);
            method.visitVarInsn(Opcodes.ALOAD, 0);
            switch (element.getElementKind()) {
            case VALUE:
                getValue(method, target, (ValueElement) element, ids);
                break;
            case OPERATOR:
            case AGGREGATE:
                getClass(method, target, (ClassNode) element, ids);
                break;
            case OUTPUT:
                getOutput(method, target, (OutputNode) element, ids);
                break;
            case DATA_TABLE:
                getDataTable(method, target, (DataTableNode) element, ids);
                break;
            case CONTEXT:
                getContext(method, target, ids);
                break;
            case EMPTY_DATA_TABLE:
                getEmptyDataTable(method, target, ids);
                break;
            default:
                throw new AssertionError(element.getElementKind());
            }
            method.visitFieldInsn(
                    Opcodes.PUTFIELD,
                    target.getInternalName(),
                    id,
                    typeOf(element.getRuntimeType()).getDescriptor());
            method.visitInsn(Opcodes.RETURN);
            method.visitMaxs(0, 0);
            method.visitEnd();
        }
    }

    private static void getContext(
            MethodVisitor method, ClassDescription target, Function<VertexElement, String> ids) {
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitFieldInsn(
                Opcodes.GETFIELD,
                target.getInternalName(),
                FIELD_CONTEXT,
                typeOf(OperationAdapter.Context.class).getDescriptor());
    }

    private static void getEmptyDataTable(
            MethodVisitor method, ClassDescription target, Function<VertexElement, String> ids) {
        method.visitMethodInsn(Opcodes.INVOKESTATIC,
                typeOf(BasicDataTable.class).getInternalName(),
                "empty", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(DataTable.class)),
                false);
    }

    private static void getValue(
            MethodVisitor method, ClassDescription target,
            ValueElement element, Function<VertexElement, String> ids) {
        ValueDescription value = element.getValue();
        switch (value.getValueKind()) {
        case IMMEDIATE:
            getConst(method, ((ImmediateDescription) value).getValue());
            break;
        case ENUM_CONSTANT:
            getEnumConstant(method, (EnumConstantDescription) value);
            break;
        case TYPE:
            getConst(method, typeOf(((TypeDescription) value).getErasure()));
            break;
        default:
            throw new UnsupportedOperationException(value.toString());
        }
    }

    private static void getClass(
            MethodVisitor method, ClassDescription target,
            ClassNode element, Function<VertexElement, String> ids) {
        method.visitTypeInsn(Opcodes.NEW, element.getImplementationType().getInternalName());
        method.visitInsn(Opcodes.DUP);
        List<Type> parameterTypes = new ArrayList<>();
        for (VertexElement dep : element.getDependencies()) {
            parameterTypes.add(typeOf(dep.getRuntimeType()));
            get(method, target, dep, ids);
        }
        method.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                element.getImplementationType().getInternalName(),
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE, parameterTypes.stream().toArray(Type[]::new)),
                false);
    }

    private static void getOutput(
            MethodVisitor method, ClassDescription target,
            OutputNode element, Function<VertexElement, String> ids) {
        getContext(method, target, ids);
        getConst(method, element.getDataType());
        getConst(method, element.getId());
        method.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                typeOf(OperationAdapter.Context.class).getInternalName(),
                "getSink",
                Type.getMethodDescriptor(typeOf(Result.class), typeOf(Class.class), typeOf(String.class)),
                true);
    }

    private static void getDataTable(
            MethodVisitor method, ClassDescription target,
            DataTableNode element, Function<VertexElement, String> ids) {
        getContext(method, target, ids);
        getConst(method, element.getDataType());
        getConst(method, element.getId());
        method.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                typeOf(OperationAdapter.Context.class).getInternalName(),
                "getDataTable",
                Type.getMethodDescriptor(typeOf(DataTable.class), typeOf(Class.class), typeOf(String.class)),
                true);
    }

    private static void get(
            MethodVisitor method, ClassDescription target,
            VertexElement element, Function<VertexElement, String> ids) {
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitFieldInsn(
                Opcodes.GETFIELD,
                target.getInternalName(),
                ids.apply(element),
                typeOf(element.getRuntimeType()).getDescriptor());
    }
}
