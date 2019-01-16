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
package com.asakusafw.dag.compiler.jdbc;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.NameUtil;
import com.asakusafw.dag.compiler.codegen.PropertyTypeKind;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.value.DateUtil;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Generates {@link ResultSetAdapter}.
 * @since 0.4.0
 */
public final class ResultSetAdapterGenerator {

    private static final String CATEGORY = "jdbc"; //$NON-NLS-1$

    private static final String SUFFIX = "RSA"; //$NON-NLS-1$

    private ResultSetAdapterGenerator() {
        return;
    }

    /**
     * Creates and adds a {@link ResultSetAdapter} class.
     * @param context the current context
     * @param spec the specification of the target class
     * @return the generated class, or cached one
     */
    public static ClassDescription get(ClassGeneratorContext context, Spec spec) {
        return context.addClassFile(generate(context, spec));
    }

    /**
     * Creates a {@link ResultSetAdapter} class.
     * @param context the current context
     * @param spec the specification of the target class
     * @return the generated class, or cached one
     */
    public static ClassData generate(ClassGeneratorContext context, Spec spec) {
        return context.cache(spec, () -> {
            ClassDescription target =
                    context.getClassName(CATEGORY, NameUtil.getSimpleNameHint(spec.dataType, SUFFIX));
            DataModelReference dataType = context.getDataModelLoader().load(spec.dataType);
            List<PropertyReference> properties = spec.properties.stream()
                    .map(name -> Arguments.requireNonNull(dataType.findProperty(name), () -> name))
                    .collect(Collectors.toList());
            return generate(target, dataType, properties);
        });
    }

    static ClassData generate(
            ClassDescription target,
            DataModelReference dataType, List<PropertyReference> properties) {
        ClassWriter writer = newWriter(target, Object.class, ResultSetAdapter.class);

        FieldRef buffer = defineField(writer, target, "buffer", typeOf(dataType.getDeclaration()));
        Optional<FieldRef> calendarBuf = properties.stream()
                .map(PropertyReference::getType)
                .map(PropertyTypeKind::fromOptionType)
                .filter(Predicate.isEqual(PropertyTypeKind.DATE).or(Predicate.isEqual(PropertyTypeKind.DATE_TIME)))
                .findAny()
                .map(k -> defineField(writer, target, "calendarBuf", typeOf(java.util.Calendar.class)));

        defineEmptyConstructor(writer, Object.class, method -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            self.load(method);
            getNew(method, dataType.getDeclaration());
            putField(method, buffer);
            calendarBuf.ifPresent(f -> {
                self.load(method);
                method.visitMethodInsn(Opcodes.INVOKESTATIC,
                        typeOf(java.util.Calendar.class).getInternalName(),
                        "getInstance", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(java.util.Calendar.class)),
                        false);
                putField(method, f);
            });
        });

        defineBody(writer, dataType, properties, buffer, calendarBuf);

        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    private static void defineBody(
            ClassWriter writer,
            DataModelReference dataType,
            List<PropertyReference> properties,
            FieldRef valueBuffer, Optional<FieldRef> calendarBuf) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "extract",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(ResultSet.class)),
                null,
                new String[] {
                        typeOf(SQLException.class).getInternalName(),
                });
        LocalVarRef rs = new LocalVarRef(Opcodes.ALOAD, 1);
        valueBuffer.load(v);
        LocalVarRef buf = putLocalVar(v, Type.OBJECT, 2);

        int columnIndex = 0;
        Set<PropertyName> sawSet = new HashSet<>();
        for (PropertyReference property : properties) {
            columnIndex++;
            Label elseIf = new Label();
            Label endIf = new Label();

            buf.load(v);
            getOption(v, property);

            rs.load(v);
            getConst(v, columnIndex);
            doGetValue(v, property, calendarBuf);

            // if (rs.wasNull()) {
            rs.load(v);
            v.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "wasNull",
                    Type.getMethodDescriptor(typeOf(boolean.class)),
                    true);
            v.visitJumpInsn(Opcodes.IFEQ, elseIf);
            if (isWideType(property)) {
                v.visitInsn(Opcodes.POP2);
            } else {
                v.visitInsn(Opcodes.POP);
            }
            doSetNull(v);
            v.visitJumpInsn(Opcodes.GOTO, endIf);

            // } else { @elseIf
            v.visitLabel(elseIf);
            doSetValue(v, property);

            // } @endIf
            v.visitLabel(endIf);

            sawSet.add(property.getName());
        }

        // resets other properties
        for (PropertyReference property : dataType.getProperties()) {
            if (sawSet.contains(property.getName())) {
                continue;
            }
            buf.load(v);
            getOption(v, property);
            doSetNull(v);
        }

        buf.load(v);
        v.visitInsn(Opcodes.ARETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void doGetValue(MethodVisitor method, PropertyReference property, Optional<FieldRef> calendarBuf) {
        PropertyTypeKind kind = PropertyTypeKind.fromOptionType(property.getType());
        switch (kind) {
        case BOOLEAN:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getBoolean", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case BYTE:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getByte", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case SHORT:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getShort", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case INT:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getInt", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case LONG:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getLong", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case FLOAT:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getFloat", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case DOUBLE:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getDouble", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case DECIMAL:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getBigDecimal", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getRawType()), typeOf(int.class)),
                    true);
            break;
        case STRING:
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getString", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(String.class), typeOf(int.class)),
                    true);
            break;
        case DATE:
            calendarBuf.get().load(method);
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getDate", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(java.sql.Date.class),
                            typeOf(int.class), typeOf(java.util.Calendar.class)),
                    true);
            break;
        case DATE_TIME:
            calendarBuf.get().load(method);
            method.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    typeOf(ResultSet.class).getInternalName(),
                    "getTimestamp", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(java.sql.Timestamp.class),
                            typeOf(int.class), typeOf(java.util.Calendar.class)),
                    true);
            break;
        default:
            throw new AssertionError(property);
        }
    }

    private static void doSetNull(MethodVisitor method) {
        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                typeOf(ValueOption.class).getInternalName(),
                "setNull", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(ValueOption.class)),
                false);
        method.visitInsn(Opcodes.POP);
    }

    private static void doSetValue(MethodVisitor method, PropertyReference property) {
        PropertyTypeKind kind = PropertyTypeKind.fromOptionType(property.getType());
        switch (kind) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
            method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    typeOf(kind.getOptionType()).getInternalName(),
                    "modify", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getOptionType()), typeOf(kind.getRawType())),
                    false);
            method.visitInsn(Opcodes.POP);
            break;
        case STRING:
            method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    typeOf(kind.getOptionType()).getInternalName(),
                    "modify", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getOptionType()), typeOf(String.class)),
                    false);
            method.visitInsn(Opcodes.POP);
            break;
        case DATE:
            method.visitMethodInsn(Opcodes.INVOKESTATIC,
                    typeOf(DateUtil.class).getInternalName(),
                    "getDayFromDate", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(int.class), typeOf(java.util.Date.class)),
                    false);
            method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    typeOf(kind.getOptionType()).getInternalName(),
                    "modify", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getOptionType()), typeOf(int.class)),
                    false);
            method.visitInsn(Opcodes.POP);
            break;
        case DATE_TIME:
            method.visitMethodInsn(Opcodes.INVOKESTATIC,
                    typeOf(DateUtil.class).getInternalName(),
                    "getSecondFromDate", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(long.class), typeOf(java.util.Date.class)),
                    false);
            method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    typeOf(kind.getOptionType()).getInternalName(),
                    "modify", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(kind.getOptionType()), typeOf(long.class)),
                    false);
            method.visitInsn(Opcodes.POP);
            break;
        default:
            throw new AssertionError(property);
        }
    }

    private static boolean isWideType(PropertyReference property) {
        switch (PropertyTypeKind.fromOptionType(property.getType())) {
        case LONG:
        case DOUBLE:
            return true;
        default:
            return false;
        }
    }

    /**
     * Specification of {@link ResultSetAdapter}.
     * @since 0.4.0
     */
    public static class Spec {

        final TypeDescription dataType;

        final List<PropertyName> properties;

        /**
         * Creates a new instance.
         * @param dataType the target data type
         * @param properties the mapped properties
         */
        public Spec(TypeDescription dataType, List<? extends PropertyName> properties) {
            Objects.requireNonNull(dataType);
            Objects.requireNonNull(properties);
            this.dataType = dataType;
            this.properties = Arguments.freeze(properties);
        }

        @Override
        public int hashCode() {
            return Lang.hashCode(dataType, properties);
        }

        @Override
        public boolean equals(Object obj) {
            return Lang.equals(this, obj, it -> it.dataType, it -> it.properties);
        }
    }
}
