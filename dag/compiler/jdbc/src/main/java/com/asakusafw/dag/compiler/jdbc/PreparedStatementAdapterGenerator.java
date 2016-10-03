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
package com.asakusafw.dag.compiler.jdbc;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Generates {@link PreparedStatementAdapter}.
 * @since 0.4.0
 */
public final class PreparedStatementAdapterGenerator {

    private static final String CATEGORY = "jdbc"; //$NON-NLS-1$

    private static final String SUFFIX = "PSA"; //$NON-NLS-1$

    private PreparedStatementAdapterGenerator() {
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
        ClassWriter writer = newWriter(target, Object.class, PreparedStatementAdapter.class);

        Optional<FieldRef> calendarBuf = properties.stream()
                .map(PropertyReference::getType)
                .map(PropertyTypeKind::fromOptionType)
                .filter(Predicate.isEqual(PropertyTypeKind.DATE).or(Predicate.isEqual(PropertyTypeKind.DATE_TIME)))
                .findAny()
                .map(k -> defineField(writer, target, "calendarBuf", typeOf(java.util.Calendar.class)));

        defineEmptyConstructor(writer, Object.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            calendarBuf.ifPresent(f -> {
                self.load(v);
                v.visitMethodInsn(Opcodes.INVOKESTATIC,
                        typeOf(java.util.Calendar.class).getInternalName(),
                        "getInstance", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(java.util.Calendar.class)),
                        false);
                putField(v, f);
            });
        });

        defineBody(writer, dataType, properties, calendarBuf);

        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    private static void defineBody(
            ClassWriter writer,
            DataModelReference dataType, List<PropertyReference> properties,
            Optional<FieldRef> dateBuf) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "drive", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(void.class), typeOf(PreparedStatement.class), typeOf(Object.class)),
                null,
                new String[] {
                        typeOf(SQLException.class).getInternalName(),
                });
        LocalVarRef row = new LocalVarRef(Opcodes.ALOAD, 1);
        LocalVarRef object = cast(v, 2, dataType.getDeclaration());

        int columnIndex = 0;
        for (PropertyReference property : properties) {
            columnIndex++;

            object.load(v);
            getOption(v, property);
            LocalVarRef option = putLocalVar(v, Type.OBJECT, 3);

            Label elseIf = new Label();
            Label endIf = new Label();

            // if (option.isNull()) {
            option.load(v);
            v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    typeOf(ValueOption.class).getInternalName(),
                    "isNull", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(boolean.class)),
                    false);
            v.visitJumpInsn(Opcodes.IFEQ, elseIf);

            row.load(v);
            getConst(v, columnIndex);
            doSetNull(v, property);

            v.visitJumpInsn(Opcodes.GOTO, endIf);

            // } else { @elseIf
            v.visitLabel(elseIf);

            row.load(v);
            getConst(v, columnIndex);
            option.load(v);
            doSetValue(v, property, dateBuf);

            // } @endIf
            v.visitLabel(endIf);
        }

        v.visitInsn(Opcodes.RETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void doSetNull(MethodVisitor method, PropertyReference property) {
        // {PreparedStatement, index:int}
        getConst(method, getSqlType(property));
        method.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                typeOf(PreparedStatement.class).getInternalName(),
                "setNull", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(void.class), typeOf(int.class), typeOf(int.class)),
                true);
    }

    private static int getSqlType(PropertyReference property) {
        PropertyTypeKind kind = PropertyTypeKind.fromOptionType(property.getType());
        switch (kind) {
        case BOOLEAN:
            return java.sql.Types.BOOLEAN;
        case BYTE:
            return java.sql.Types.TINYINT;
        case SHORT:
            return java.sql.Types.SMALLINT;
        case INT:
            return java.sql.Types.INTEGER;
        case LONG:
            return java.sql.Types.BIGINT;
        case FLOAT:
            return java.sql.Types.FLOAT;
        case DOUBLE:
            return java.sql.Types.DOUBLE;
        case DECIMAL:
            return java.sql.Types.DECIMAL;
        case STRING:
            return java.sql.Types.VARCHAR;
        case DATE:
            return java.sql.Types.DATE;
        case DATE_TIME:
            return java.sql.Types.TIMESTAMP;
        default:
            throw new AssertionError(property);
        }
    }

    private static void doSetValue(MethodVisitor method, PropertyReference property, Optional<FieldRef> calendarBuf) {
        // {PreparedStatement, index:int, +ValueOption}
        PropertyTypeKind kind = PropertyTypeKind.fromOptionType(property.getType());
        switch (kind) {
        case BOOLEAN:
            doSetValue(method, kind, "setBoolean");
            break;
        case BYTE:
            doSetValue(method, kind, "setByte");
            break;
        case SHORT:
            doSetValue(method, kind, "setShort");
            break;
        case INT:
            doSetValue(method, kind, "setInt");
            break;
        case LONG:
            doSetValue(method, kind, "setLong");
            break;
        case FLOAT:
            doSetValue(method, kind, "setFloat");
            break;
        case DOUBLE:
            doSetValue(method, kind, "setDouble");
            break;
        case DECIMAL:
            doSetValue(method, kind, "setBigDecimal");
            break;
        case STRING:
            doSetString(method, kind);
            break;
        case DATE:
            doSetDateLike(method, kind, calendarBuf.get());
            break;
        case DATE_TIME:
            doSetDateLike(method, kind, calendarBuf.get());
            break;
        default:
            throw new AssertionError(property);
        }
    }

    private static void doSetValue(MethodVisitor method, PropertyTypeKind kind, String setter) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(kind.getOptionType()).getInternalName(),
                "get", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(kind.getRawType())),
                false);
        method.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                typeOf(PreparedStatement.class).getInternalName(),
                setter,
                Type.getMethodDescriptor(typeOf(void.class), typeOf(int.class), typeOf(kind.getRawType())),
                true);
    }

    private static void doSetString(MethodVisitor method, PropertyTypeKind kind) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(kind.getOptionType()).getInternalName(),
                "getAsString", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(String.class)),
                false);
        method.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                typeOf(PreparedStatement.class).getInternalName(),
                "setString",
                Type.getMethodDescriptor(typeOf(void.class), typeOf(int.class), typeOf(String.class)),
                true);
    }

    private static void doSetDateLike(MethodVisitor method, PropertyTypeKind kind, FieldRef calendarBuf) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(kind.getOptionType()).getInternalName(),
                "get", //$NON-NLS-1$
                Type.getMethodDescriptor(typeOf(kind.getRawType())),
                false);

        calendarBuf.load(method);
        method.visitMethodInsn(Opcodes.INVOKESTATIC,
                typeOf(JdbcUtil.class).getInternalName(),
                "setParameter",
                Type.getMethodDescriptor(typeOf(void.class),
                        typeOf(PreparedStatement.class),
                        typeOf(int.class),
                        typeOf(kind.getRawType()),
                        typeOf(java.util.Calendar.class)),
                true);
    }

    /**
     * Specification of {@link PreparedStatementAdapter}.
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
