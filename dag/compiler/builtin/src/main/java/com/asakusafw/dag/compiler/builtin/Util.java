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

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil;
import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.Context;
import com.asakusafw.dag.compiler.model.graph.DataNode;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.skeleton.CoGroupOperationUtil;
import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.value.ValueOption;

final class Util {

    private Util() {
        return;
    }

    static void checkPorts(Operator operator, IntPredicate inputs, IntPredicate outputs) {
        Invariants.require(inputs.test(operator.getInputs().size()));
        Invariants.require(outputs.test(operator.getOutputs().size()));
    }

    static void checkArgs(Operator operator, IntPredicate args) {
        Invariants.require(args.test(operator.getArguments().size()));
    }

    static void checkDependencies(Context context, Operator operator) {
        for (OperatorInput port : operator.getInputs()) {
            if (context.isSideData(port)) {
                VertexElement element = context.getDependency(port);
                checkDependency(element, port);
            }
        }
        for (OperatorOutput port : operator.getOutputs()) {
            VertexElement element = context.getDependency(port);
            checkDependency(element, port);
        }
    }

    private static void checkDependency(VertexElement element, OperatorPort port) {
        Invariants.require(element instanceof DataNode);
        TypeDescription type = ((DataNode) element).getDataType();
        Invariants.require(type.equals(port.getDataType()));
    }

    static FieldRef defineOperatorField(ClassVisitor writer, UserOperator operator, ClassDescription target) {
        return AsmUtil.defineField(writer, target, "impl", AsmUtil.typeOf(operator.getImplementationClass()));
    }

    static void setOperatorField(MethodVisitor method, UserOperator operator, FieldRef field) {
        method.visitVarInsn(Opcodes.ALOAD, 0);
        AsmUtil.getNew(method, operator.getImplementationClass());
        AsmUtil.putField(method, field);
    }

    static Map<OperatorProperty, FieldRef> defineConstructor(
            Context context,
            Operator operator,
            ClassDescription aClass,
            ClassVisitor writer,
            Consumer<MethodVisitor> body) {
        return defineConstructor(context, operator, aClass, writer, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    AsmUtil.typeOf(Object.class).getInternalName(),
                    "<init>",
                    Type.getMethodDescriptor(Type.VOID_TYPE),
                    false);
        }, body);
    }

    static Map<OperatorProperty, FieldRef> defineConstructor(
            Context context,
            Operator operator,
            ClassDescription aClass,
            ClassVisitor writer,
            Consumer<MethodVisitor> superConstructor,
            Consumer<MethodVisitor> body) {
        List<OperatorProperty> properties = Lang.concat(operator.getOutputs(), operator.getArguments());
        return defineConstructor(context, properties, aClass, writer, superConstructor, body);
    }

    static Map<OperatorProperty, FieldRef> defineConstructor(
            Context context,
            List<? extends OperatorProperty> properties,
            ClassDescription aClass,
            ClassVisitor writer,
            Consumer<MethodVisitor> superConstructor,
            Consumer<MethodVisitor> body) {
        Map<OperatorProperty, VertexElement> elements = new LinkedHashMap<>();
        for (OperatorProperty property : properties) {
            elements.put(property, context.getDependency(property));
        }
        Map<VertexElement, FieldRef> deps = AsmUtil.defineDependenciesConstructor(
                aClass, writer, elements.values(), superConstructor, body);
        Map<OperatorProperty, FieldRef> results = new LinkedHashMap<>();
        elements.forEach((k, v) -> results.put(k, Invariants.requireNonNull(deps.get(v))));
        return results;
    }

    static List<VertexElement> getDefaultDependencies(Context context, UserOperator operator) {
        return context.getDependencies(Lang.concat(operator.getOutputs(), operator.getArguments()));
    }

    static void getGroupList(MethodVisitor method, Context context, OperatorInput input) {
        method.visitVarInsn(Opcodes.ALOAD, 1);
        getInt(method, context.getGroupIndex(input));
        method.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                AsmUtil.typeOf(CoGroupOperationUtil.class).getInternalName(),
                "getList",
                Type.getMethodDescriptor(AsmUtil.typeOf(List.class),
                        AsmUtil.typeOf(CoGroupOperation.Input.class), Type.INT_TYPE),
                false);
    }

    static void invoke(MethodVisitor method, Context context, UserOperator operator, List<ValueRef> arguments) {
        for (ValueRef var : arguments) {
            var.load(method);
        }
        Method ref = Invariants.safe(() -> operator.getMethod().resolve(context.getClassLoader()));
        Invariants.require(Modifier.isStatic(ref.getModifiers()) == false);
        Invariants.require(ref.getDeclaringClass().isInterface() == false);
        Invariants.require(arguments.size() == ref.getParameterCount() + 1);
        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                AsmUtil.typeOf(ref.getDeclaringClass()).getInternalName(),
                ref.getName(),
                Type.getMethodDescriptor(ref),
                false);
    }

    static void mapping(
            MethodVisitor method,
            DataModelLoader loader,
            List<PropertyMapping> mappings,
            Map<OperatorInput, ValueRef> inputRefs,
            Map<OperatorOutput, ValueRef> outputRefs) {
        Map<OperatorInput, DataModelReference> inputTypes = inputRefs.keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        p -> Invariants.safe(() -> loader.load(p.getDataType()))));
        Map<OperatorOutput, DataModelReference> outputTypes = outputRefs.keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        p -> Invariants.safe(() -> loader.load(p.getDataType()))));
        Map<OperatorOutput, Map<PropertyName, PropertyReference>> open = new HashMap<>();
        outputTypes.forEach((p, m) -> open.put(p, m.getProperties().stream()
                .collect(Collectors.toMap(
                        PropertyReference::getName,
                        Function.identity()))));

        for (PropertyMapping mapping : mappings) {
            ValueRef srcRef = Invariants.requireNonNull(inputRefs.get(mapping.getSourcePort()));
            PropertyReference srcProp = Invariants.requireNonNull(
                    inputTypes.get(mapping.getSourcePort()).findProperty(mapping.getSourceProperty()));
            ValueRef dstRef = Invariants.requireNonNull(outputRefs.get(mapping.getDestinationPort()));
            PropertyReference dstProp = Invariants.requireNonNull(
                    outputTypes.get(mapping.getDestinationPort()).findProperty(mapping.getDestinationProperty()));
            Invariants.require(srcProp.getType().equals(dstProp.getType()));
            open.get(mapping.getDestinationPort()).remove(mapping.getDestinationProperty());

            dstRef.load(method);
            AsmUtil.getOption(method, dstProp);

            srcRef.load(method);
            AsmUtil.getOption(method, srcProp);

            AsmUtil.copyOption(method, srcProp.getType());
        }
        open.forEach((p, map) -> {
            ValueRef ref = outputRefs.get(p);
            // always non null?
            if (ref != null) {
                map.forEach((name, prop) -> {
                    ref.load(method);
                    AsmUtil.getOption(method, prop);
                    method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            typeOf(ValueOption.class).getInternalName(), "setNull",
                            Type.getMethodDescriptor(typeOf(ValueOption.class)),
                            false);

                });
            }
        });
    }

    static void defineBuildKey(
            ClassGeneratorContext context,
            ClassWriter writer,
            TypeDescription dataType, Group group) {
        DataModelReference type = context.getDataModelLoader().load(dataType);
        List<PropertyReference> props = group.getGrouping().stream()
                .map(p -> Invariants.requireNonNull(type.findProperty(p)))
                .collect(Collectors.toList());

        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL,
                "buildKey",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(KeyBuffer.class), typeOf(Object.class)),
                null,
                null);

        LocalVarRef key = new LocalVarRef(Opcodes.ALOAD, 1);
        LocalVarRef object = cast(v, 2, dataType);
        for (PropertyReference p : props) {
            key.load(v);
            object.load(v);
            getOption(v, p);
            v.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(KeyBuffer.class).getInternalName(), "append",
                    Type.getMethodDescriptor(typeOf(KeyBuffer.class), typeOf(Object.class)),
                    true);
            v.visitInsn(Opcodes.POP);
        }

        v.visitInsn(Opcodes.RETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }
}
