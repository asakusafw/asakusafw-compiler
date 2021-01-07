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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Generates {@link Comparator}.
 * @since 0.4.1
 */
public final class ObjectComparatorGenerator {

    private static final String CATEGORY = "compare"; //$NON-NLS-1$

    private static final String SUFFIX = "ObjectComparator"; //$NON-NLS-1$

    private static final Type TYPE_COMPARABLE = typeOf(Comparable.class);

    private ObjectComparatorGenerator() {
        return;
    }

    /**
     * Generates {@link Comparator} class.
     * @param context the current context
     * @param type the target data model type
     * @param orderings the ordering terms
     * @return the generated class
     */
    public static ClassDescription get(
            ClassGeneratorContext context, TypeDescription type, List<Group.Ordering> orderings) {
        return context.addClassFile(generate(context, type, orderings));
    }

    /**
     * Generates {@link Comparator} class.
     * @param context the current context
     * @param type the target data model type
     * @param orderings the ordering terms
     * @return the generated class data
     */
    public static ClassData generate(
            ClassGeneratorContext context, TypeDescription type, List<Group.Ordering> orderings) {
        return context.cache(new Key(type, orderings), () -> {
            DataModelReference ref = context.getDataModelLoader().load(type);
            ClassDescription target = context.getClassName(CATEGORY, NameUtil.getSimpleNameHint(type, SUFFIX));
            return generate0(ref, orderings, target);
        });
    }

    private static ClassData generate0(
            DataModelReference reference, List<Group.Ordering> orderings, ClassDescription target) {
        ClassWriter writer = newWriter(target, Object.class, Comparator.class);
        defineEmptyConstructor(writer, Object.class);
        defineCompare(writer, reference, orderings);
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    private static void defineCompare(
            ClassWriter writer, DataModelReference reference, List<Group.Ordering> orderings) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "compare",
                Type.getMethodDescriptor(typeOf(int.class), typeOf(Object.class), typeOf(Object.class)),
                null,
                null);
        LocalVarRef a = cast(v, 1, reference.getDeclaration());
        LocalVarRef b = cast(v, 2, reference.getDeclaration());
        for (Group.Ordering ordering : orderings) {
            LocalVarRef left;
            LocalVarRef right;
            switch (ordering.getDirection()) {
            case ASCENDANT:
                left = a;
                right = b;
                break;
            case DESCENDANT:
                left = b;
                right = a;
                break;
            default:
                throw new AssertionError(ordering);
            }

            // int diff = left.getXOption().compareTo(right.getXOption());
            PropertyReference property = Invariants.requireNonNull(reference.findProperty(ordering.getPropertyName()));
            left.load(v);
            getOption(v, property);
            right.load(v);
            getOption(v, property);
            v.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                    TYPE_COMPARABLE.getInternalName(),
                    "compareTo",
                    Type.getMethodDescriptor(typeOf(int.class), typeOf(Object.class)),
                    true);
            LocalVarRef cmp = putLocalVar(v, Type.INT, 3);
            Label eq = new Label();

            // if (diff != 0) {
            cmp.load(v);
            v.visitJumpInsn(Opcodes.IFEQ, eq);

            // return diff;
            cmp.load(v);
            v.visitInsn(Opcodes.IRETURN);

            // } @ eq
            v.visitLabel(eq);
        }
        getConst(v, 0);
        v.visitInsn(Opcodes.IRETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static class Key {

        private final TypeDescription type;

        private final List<Group.Ordering> orderings;

        Key(TypeDescription type, List<Group.Ordering> orderings) {
            this.type = type;
            this.orderings = Arguments.freeze(orderings);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = Key.class.hashCode();
            result = prime * result + Objects.hashCode(type);
            result = prime * result + Objects.hashCode(orderings);
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
            if (!Objects.equals(orderings, other.orderings)) {
                return false;
            }
            return true;
        }
    }
}
