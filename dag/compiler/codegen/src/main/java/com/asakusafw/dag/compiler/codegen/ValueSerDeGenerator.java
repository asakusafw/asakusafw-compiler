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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Generates {@link ValueSerDe}.
 * @since 0.4.0
 */
public final class ValueSerDeGenerator {

    private static final ClassDescription SERDE = Descriptions.classOf(ValueOptionSerDe.class);

    private static final String CATEGORY = "serde"; //$NON-NLS-1$

    private static final String SUFFIX = "SerDe"; //$NON-NLS-1$

    private ValueSerDeGenerator() {
        return;
    }

    /**
     * Generates {@link ValueSerDe} class.
     * @param context the current context
     * @param type the target data model type
     * @return the generated class
     */
    public static ClassDescription get(ClassGeneratorContext context, TypeDescription type) {
        return context.addClassFile(generate(context, type));
    }

    /**
     * Generates {@link ValueSerDe} class.
     * @param context the current context
     * @param type the target data model type
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, TypeDescription type) {
        return context.cache(new Key(type), () -> {
            DataModelReference ref = context.getDataModelLoader().load(type);
            ClassDescription target = context.getClassName(CATEGORY, NameUtil.getSimpleNameHint(type, SUFFIX));
            return generate0(ref, target);
        });
    }

    private static ClassData generate0(DataModelReference reference, ClassDescription target) {
        ClassWriter writer = newWriter(target, Object.class, ValueSerDe.class);
        FieldRef buffer = defineField(writer, target, "buffer", typeOf(reference));
        defineEmptyConstructor(writer, Object.class, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(v, reference.getDeclaration());
            putField(v, buffer);
        });
        putSerialize(reference, writer);
        putDeserialize(reference, buffer, writer);
        return new ClassData(target, writer::toByteArray);
    }

    private static void putSerialize(DataModelReference reference, ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "serialize",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class), typeOf(DataOutput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        LocalVarRef object = cast(v, 1, reference.getDeclaration());
        LocalVarRef output = new LocalVarRef(Opcodes.ALOAD, 2);
        for (PropertyReference property : reference.getProperties()) {
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
        v.visitInsn(Opcodes.RETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void putDeserialize(DataModelReference reference, FieldRef buffer, ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "deserialize",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(DataInput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
        LocalVarRef input = new LocalVarRef(Opcodes.ALOAD, 1);

        self.load(v);
        getField(v, buffer);
        LocalVarRef object = putLocalVar(v, Type.OBJECT, 2);
        for (PropertyReference property : reference.getProperties()) {
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
        object.load(v);
        v.visitInsn(Opcodes.ARETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static class Key {

        private final TypeDescription type;

        Key(TypeDescription type) {
            this.type = type;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(type);
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
            return Objects.equals(type, other.type);
        }

        @Override
        public String toString() {
            return String.format("ValueSerDe(%s)", type); //$NON-NLS-1$
        }
    }
}
