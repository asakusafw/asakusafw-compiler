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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.List;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.KeyExtractor;
import com.asakusafw.dag.runtime.skeleton.EdgeDataTableAdapter;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Generates {@link EdgeDataTableAdapter}.
 * @since 0.4.0
 * @version 0.4.1
 */
public class EdgeDataTableAdapterGenerator {

    /**
     * Generates {@link EdgeDataTableAdapter} class.
     * @param context the current context
     * @param specs the target output ports
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, List<Spec> specs, ClassDescription target) {
        ClassWriter writer = AsmUtil.newWriter(target, EdgeDataTableAdapter.class);
        defineAdapterConstructor(writer, EdgeDataTableAdapter.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            int index = 0;
            for (Spec spec : specs) {
                ClassDescription keyBuilder;
                if (spec.group.getGrouping().isEmpty()) {
                    keyBuilder = null;
                } else {
                    keyBuilder = generateKeyBuilder(context, spec, qualify(target, "k", index));
                }
                ClassDescription copier = ObjectCopierGenerator.get(context, spec.dataType);
                ClassDescription comparator = toComparatorClass(context, spec);
                TypeDescription[] keyElementTypes = toKeyElementTypes(context, spec);
                self.load(v);
                getConst(v, spec.tableId);
                getConst(v, spec.inputId);
                getConst(v, keyBuilder);
                getConst(v, copier);
                getConst(v, comparator);
                getArray(v, typeOf(Class.class), keyElementTypes);
                v.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        target.getInternalName(), "bind",
                        Type.getMethodDescriptor(
                                typeOf(EdgeDataTableAdapter.class),
                                typeOf(String.class), typeOf(String.class),
                                typeOf(Class.class), typeOf(Class.class), typeOf(Class.class),
                                typeOf(Class[].class)),
                        false);
                v.visitInsn(Opcodes.POP);
                index++;
            }
        });
        return new ClassData(target, writer::toByteArray);
    }

    static ClassDescription toComparatorClass(ClassGeneratorContext context, Spec spec) {
        ClassDescription comparator;
        if (spec.group.getOrdering().isEmpty()) {
            comparator = null;
        } else {
            comparator = ObjectComparatorGenerator.get(context, spec.dataType, spec.group.getOrdering());
        }
        return comparator;
    }

    static TypeDescription[] toKeyElementTypes(ClassGeneratorContext context, Spec spec) {
        DataModelReference dataModel = context.getDataModelLoader().load(spec.dataType);
        return spec.group.getGrouping().stream()
                .sequential()
                .map(n -> Invariants.requireNonNull(dataModel.findProperty(n)))
                .map(PropertyReference::getType)
                .toArray(TypeDescription[]::new);
    }

    ClassDescription generateKeyBuilder(ClassGeneratorContext context, Spec spec, ClassDescription target) {
        ClassWriter writer = AsmUtil.newWriter(target, Object.class, KeyExtractor.class);
        defineEmptyConstructor(writer, Object.class);
        defineBuildKey(context, writer, spec.dataType, spec.group);
        return context.addClassFile(new ClassData(target, writer::toByteArray));
    }

    private static void defineBuildKey(
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

    private static ClassDescription qualify(ClassDescription base, String category, int index) {
        return new ClassDescription(new StringBuilder()
                .append(base.getBinaryName())
                .append('_')
                .append(category)
                .append(index)
                .toString());
    }

    /**
     * Represents operation of {@link EdgeDataTableAdapterGenerator}.
     * @since 0.4.0
     */
    public static class Spec {

        final String tableId;

        final String inputId;

        final TypeDescription dataType;

        final Group group;

        /**
         * Creates a new instance.
         * @param tableId the table ID
         * @param inputId the source input ID
         * @param dataType the data type
         * @param group the grouping info
         */
        public Spec(String tableId, String inputId, TypeDescription dataType, Group group) {
            Arguments.requireNonNull(tableId);
            Arguments.requireNonNull(inputId);
            Arguments.requireNonNull(dataType);
            Arguments.requireNonNull(group);
            this.tableId = tableId;
            this.inputId = inputId;
            this.dataType = dataType;
            this.group = group;
        }
    }
}
