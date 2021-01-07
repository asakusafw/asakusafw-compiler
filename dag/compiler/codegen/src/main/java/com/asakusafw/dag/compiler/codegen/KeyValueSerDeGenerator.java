/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Generates {@link KeyValueSerDe} class.
 * @since 0.4.0
 */
public final class KeyValueSerDeGenerator {

    static final ClassDescription SERDE = Descriptions.classOf(ValueOptionSerDe.class);

    private static final String CATEGORY = "serde"; //$NON-NLS-1$

    private static final String SUFFIX = "KvSerDe"; //$NON-NLS-1$

    private KeyValueSerDeGenerator() {
        return;
    }

    /**
     * Generates {@link KeyValueSerDe} class.
     * @param context the current context
     * @param type the target data model type
     * @param grouping the grouping information
     * @return the generated class
     */
    public static ClassDescription get(ClassGeneratorContext context, TypeDescription type, Group grouping) {
        return context.addClassFile(generate(context, type, grouping));
    }

    /**
     * Generates {@link KeyValueSerDe} class.
     * @param context the current context
     * @param type the target data model type
     * @param grouping the grouping information
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, TypeDescription type, Group grouping) {
        return context.cache(new Key(type, grouping), () -> {
            DataModelReference ref = context.getDataModelLoader().load(type);
            ClassDescription target = context.getClassName(CATEGORY, NameUtil.getSimpleNameHint(type, SUFFIX));
            return generate0(ref, grouping, target);
        });
    }

    private static ClassData generate0(DataModelReference reference, Group grouping, ClassDescription target) {
        List<PropertyReference> keys = Lang.project(
                grouping.getGrouping(),
                n -> Invariants.requireNonNull(reference.findProperty(n)));
        List<PropertyReference> values = collectValues(reference, grouping);
        ClassWriter writer = newWriter(target, Object.class, KeyValueSerDe.class);
        FieldRef buffer = defineField(writer, target, "buffer", typeOf(reference));
        defineEmptyConstructor(writer, Object.class, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(v, reference.getDeclaration());
            putField(v, buffer);
        });
        putSerialize("serializeKey", reference, keys, writer);
        putSerialize("serializeValue", reference, values, writer);
        putDeserialize(reference, keys, values, buffer, writer);
        return new ClassData(target, writer::toByteArray);
    }

    private static List<PropertyReference> collectValues(DataModelReference reference, Group grouping) {
        List<PropertyReference> results = new ArrayList<>();
        Set<PropertyName> saw = new HashSet<>();
        saw.addAll(grouping.getGrouping());
        grouping.getOrdering().stream()
            .map(Group.Ordering::getPropertyName)
            .peek(saw::add)
            .map(n -> Invariants.requireNonNull(reference.findProperty(n)))
            .forEach(results::add);
        reference.getProperties().stream()
            .filter(p -> saw.contains(p.getName()) == false)
            .forEach(results::add);
        return results;
    }

    private static void putSerialize(
            String methodName,
            DataModelReference reference, List<PropertyReference> properties,
            ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                methodName,
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class), typeOf(DataOutput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        if (properties.isEmpty()) {
            LocalVarRef output = new LocalVarRef(Opcodes.ALOAD, 2);
            output.load(v);
            getConst(v, 0);
            v.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(DataOutput.class).getInternalName(),
                    "writeByte",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE),
                    true);
        } else {
            LocalVarRef object = cast(v, 1, reference.getDeclaration());
            LocalVarRef output = new LocalVarRef(Opcodes.ALOAD, 2);
            for (PropertyReference property : properties) {
                object.load(v);
                getOption(v, property);
                output.load(v);
                v.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        SERDE.getInternalName(),
                        "serialize",
                        Type.getMethodDescriptor(
                                Type.VOID_TYPE,
                                typeOf(property.getType()),
                                typeOf(DataOutput.class)),
                        false);
            }
        }
        v.visitInsn(Opcodes.RETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void putDeserialize(
            DataModelReference reference,
            List<PropertyReference> keys, List<PropertyReference> values,
            FieldRef buffer, ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "deserializePair",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(DataInput.class), typeOf(DataInput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
        LocalVarRef keyInput = new LocalVarRef(Opcodes.ALOAD, 1);
        LocalVarRef valueInput = new LocalVarRef(Opcodes.ALOAD, 2);
        self.load(v);
        getField(v, buffer);
        LocalVarRef object = putLocalVar(v, Type.OBJECT, 3);
        putDeserializeBody(v, keys, keyInput, object);
        putDeserializeBody(v, values, valueInput, object);
        object.load(v);
        v.visitInsn(Opcodes.ARETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void putDeserializeBody(MethodVisitor v, List<PropertyReference> props,
            LocalVarRef input, LocalVarRef object) {
        if (props.isEmpty()) {
            input.load(v);
            v.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(DataInput.class).getInternalName(),
                    "readByte",
                    Type.getMethodDescriptor(Type.BYTE_TYPE),
                    true);
            v.visitInsn(Opcodes.POP);
        } else {
            for (PropertyReference property : props) {
                object.load(v);
                getOption(v, property);
                input.load(v);
                v.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        SERDE.getInternalName(),
                        "deserialize",
                        Type.getMethodDescriptor(
                                Type.VOID_TYPE,
                                typeOf(property.getType()),
                                typeOf(DataInput.class)),
                        false);
            }
        }
    }

    private static class Key {

        private final TypeDescription type;

        private final Group group;

        Key(TypeDescription type, Group group) {
            this.type = type;
            this.group = group;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = Key.class.hashCode();
            result = prime * result + Objects.hashCode(type);
            result = prime * result + Objects.hashCode(group);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            if (!Objects.equals(type, other.type)) {
                return false;
            }
            if (!Objects.equals(group, other.group)) {
                return false;
            }
            return true;
        }
    }
}
