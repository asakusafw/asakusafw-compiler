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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;

/**
 * Generates {@link DataComparator}.
 * @since 0.4.0
 */
public final class DataComparatorGenerator {

    private static final String CATEGORY = "compare"; //$NON-NLS-1$

    private static final String SUFFIX = "DataComparator"; //$NON-NLS-1$

    private static final Type TYPE_DATA_INPUT = typeOf(DataInput.class);

    private static final String DESC_COMPARE = Type.getMethodDescriptor(
            typeOf(int.class), TYPE_DATA_INPUT, TYPE_DATA_INPUT);

    private static final Map<TypeDescription, String> METHOD_NAMES;
    static {
        Map<TypeDescription, String> map = new HashMap<>();
        map.put(Descriptions.typeOf(BooleanOption.class), "compareBoolean");
        map.put(Descriptions.typeOf(ByteOption.class), "compareByte");
        map.put(Descriptions.typeOf(ShortOption.class), "compareShort");
        map.put(Descriptions.typeOf(IntOption.class), "compareInt");
        map.put(Descriptions.typeOf(LongOption.class), "compareLong");
        map.put(Descriptions.typeOf(FloatOption.class), "compareFloat");
        map.put(Descriptions.typeOf(DoubleOption.class), "compareDouble");
        map.put(Descriptions.typeOf(DecimalOption.class), "compareDecimal");
        map.put(Descriptions.typeOf(DateOption.class), "compareDate");
        map.put(Descriptions.typeOf(DateTimeOption.class), "compareDateTime");
        map.put(Descriptions.typeOf(StringOption.class), "compareString");
        METHOD_NAMES = Collections.unmodifiableMap(map);
    }

    private DataComparatorGenerator() {
        return;
    }

    /**
     * Generates {@link DataComparator} class.
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
     * Generates {@link DataComparator} class.
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
        ClassWriter writer = newWriter(target, Object.class, DataComparator.class);
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
                DESC_COMPARE,
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                });
        LocalVarRef a = new LocalVarRef(Opcodes.ALOAD, 1);
        LocalVarRef b = new LocalVarRef(Opcodes.ALOAD, 2);
        for (Group.Ordering ordering : orderings) {
            PropertyReference property = Invariants.requireNonNull(reference.findProperty(ordering.getPropertyName()));

            // int diff = ValueOptionSerDe.compareT({a, b}, {b, a});
            switch (ordering.getDirection()) {
            case ASCENDANT:
                a.load(v);
                b.load(v);
                break;
            case DESCENDANT:
                b.load(v);
                a.load(v);
                break;
            default:
                throw new AssertionError(ordering);
            }
            v.visitMethodInsn(Opcodes.INVOKESTATIC,
                    typeOf(ValueOptionSerDe.class).getInternalName(),
                    Invariants.requireNonNull(METHOD_NAMES.get(property.getType())),
                    DESC_COMPARE,
                    false);
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
