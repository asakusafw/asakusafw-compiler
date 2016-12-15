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
package com.asakusafw.dag.compiler.codegen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ArrayTypeDescription;
import com.asakusafw.lang.compiler.model.description.BasicTypeDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Utilities for ASM.
 * @since 0.4.0
 * @version 0.4.1
 */
public final class AsmUtil {

    /**
     * The constructor method name.
     */
    public static final String CONSTRUCTOR_NAME = "<init>"; //$NON-NLS-1$

    private static final ClassDescription RESULT_TYPE = Descriptions.classOf(Result.class);

    private static final ClassDescription VALUE_OPTION_TYPE = Descriptions.classOf(ValueOption.class);

    private AsmUtil() {
        return;
    }

    /**
     * Returns the ASM {@link Type} object.
     * @param aClass the Java reflective object
     * @return the corresponded {@link Type} object
     */
    public static Type typeOf(Class<?> aClass) {
        Arguments.requireNonNull(aClass);
        return Type.getType(aClass);
    }

    /**
     * Returns the ASM {@link Type} object.
     * @param type the target type description
     * @return the corresponded {@link Type} object
     */
    public static Type typeOf(TypeDescription type) {
        Arguments.requireNonNull(type);
        TypeDescription e = type.getErasure();
        switch (e.getTypeKind()) {
        case BASIC:
            switch (((BasicTypeDescription) e).getBasicTypeKind()) {
            case BOOLEAN:
                return Type.BOOLEAN_TYPE;
            case BYTE:
                return Type.BYTE_TYPE;
            case CHAR:
                return Type.CHAR_TYPE;
            case DOUBLE:
                return Type.DOUBLE_TYPE;
            case FLOAT:
                return Type.FLOAT_TYPE;
            case INT:
                return Type.INT_TYPE;
            case LONG:
                return Type.LONG_TYPE;
            case SHORT:
                return Type.SHORT_TYPE;
            case VOID:
                return Type.VOID_TYPE;
            default:
                throw new AssertionError(type);
            }
        case ARRAY:
            return Type.getType('[' + typeOf(((ArrayTypeDescription) e).getComponentType()).getDescriptor());
        case CLASS:
            return Type.getObjectType(((ClassDescription) e).getInternalName());
        default:
            throw new AssertionError(type);
        }
    }

    /**
     * Returns the ASM {@link Type} object.
     * @param reference the data model reference
     * @return the corresponded {@link Type} object
     */
    public static Type typeOf(DataModelReference reference) {
        Arguments.requireNonNull(reference);
        return typeOf(reference.getDeclaration());
    }

    /**
     * Returns the type category.
     * @param type the target type
     * @return the category
     */
    public static int categoryOf(TypeDescription type) {
        Arguments.requireNonNull(type);
        if (type.getTypeKind() == TypeKind.BASIC) {
            switch (((BasicTypeDescription) type).getBasicTypeKind()) {
            case VOID:
                return 0;
            case LONG:
            case DOUBLE:
                return 2;
            default:
                return 1;
            }
        }
        return 1;
    }

    /**
     * Returns the T_LOAD opcode.
     * @param type the target type
     * @return the opcode
     */
    public static int loadOpcodeOf(TypeDescription type) {
        Arguments.requireNonNull(type);
        if (type.getTypeKind() == TypeKind.BASIC) {
            switch (((BasicTypeDescription) type).getBasicTypeKind()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
                return Opcodes.ILOAD;
            case LONG:
                return Opcodes.LLOAD;
            case FLOAT:
                return Opcodes.FLOAD;
            case DOUBLE:
                return Opcodes.DLOAD;
            default:
                throw new AssertionError(type);
            }
        }
        return Opcodes.ALOAD;
    }

    /**
     * Creates a new {@link ClassWriter}.
     * @param aClass the target class
     * @param superClass the super class
     * @param interfaces the interface types
     * @return the created class writer
     */
    public static ClassWriter newWriter(ClassDescription aClass, Class<?> superClass, Class<?>... interfaces) {
        Arguments.require(superClass.isInterface() == false);
        Lang.forEach(interfaces, c -> Arguments.require(c.isInterface()));
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                aClass.getInternalName(),
                null,
                typeOf(superClass).getInternalName(),
                Stream.of(interfaces)
                    .map(c -> typeOf(c).getInternalName())
                    .toArray(String[]::new));
        return writer;
    }

    /**
     * Adds an empty constructor.
     * @param writer the target class
     * @param superClass the super class
     */
    public static void defineEmptyConstructor(ClassWriter writer, Class<?> superClass) {
        defineEmptyConstructor(writer, superClass, Lang.discard());
    }

    /**
     * Adds an empty constructor.
     * @param writer the target class
     * @param block the constructor block
     */
    public static void defineEmptyConstructor(ClassWriter writer, Consumer<MethodVisitor> block) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE),
                null,
                null);
        block.accept(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds an empty constructor.
     * @param writer the target class
     * @param superClass the super class
     * @param body the constructor body
     */
    public static void defineEmptyConstructor(ClassWriter writer, Class<?> superClass, Consumer<MethodVisitor> body) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE),
                null,
                null);
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                typeOf(superClass).getInternalName(),
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE),
                false);
        body.accept(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds an adapter constructor.
     * @param writer the target class
     * @param superClass the super class
     * @param body the constructor body
     */
    public static void defineAdapterConstructor(
            ClassWriter writer,
            Class<?> superClass,
            Consumer<MethodVisitor> body) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(VertexProcessorContext.class)),
                null,
                null);
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitVarInsn(Opcodes.ALOAD, 1);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                typeOf(superClass).getInternalName(),
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(VertexProcessorContext.class)),
                false);
        body.accept(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds a getter-like method.
     * @param writer the target class
     * @param type the method result type
     * @param name the method name
     * @param body the method body
     */
    public static void defineGetter(ClassWriter writer, Type type, String name, Consumer<MethodVisitor> body) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                name,
                Type.getMethodDescriptor(type),
                null,
                new String[0]);
        body.accept(method);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds a constant value on to the top of the stack.
     * @param method the current method visitor
     * @param value the target value
     */
    public static void getConst(MethodVisitor method, Object value) {
        if (value == null) {
            method.visitInsn(Opcodes.ACONST_NULL);
        } else if (value instanceof Boolean) {
            getInt(method, ((Boolean) value).booleanValue() ? 1 : 0);
        } else if (value instanceof Byte) {
            getInt(method, ((Byte) value).byteValue());
        } else if (value instanceof Short) {
            getInt(method, ((Short) value).shortValue());
        } else if (value instanceof Character) {
            getInt(method, ((Character) value).charValue());
        } else if (value instanceof Integer) {
            getInt(method, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            getLong(method, ((Long) value).longValue());
        } else if (value instanceof Float) {
            getFloat(method, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            getDouble(method, ((Double) value).doubleValue());
        } else if (value instanceof ImmediateDescription) {
            getConst(method, ((ImmediateDescription) value).getValue());
        } else if (value instanceof EnumConstantDescription) {
            getEnumConstant(method, (EnumConstantDescription) value);
        } else if (value instanceof TypeDescription) {
            method.visitLdcInsn(typeOf((TypeDescription) value));
        } else {
            method.visitLdcInsn(value);
        }
    }

    /**
     * Adds a string array on to the top of the stack.
     * @param method the current method visitor
     * @param values the array elements
     */
    public static void getArray(MethodVisitor method, String[] values) {
        getArray(method, typeOf(String.class), values);
    }

    /**
     * Adds a string array on to the top of the stack.
     * @param method the current method visitor
     * @param elementType the element type
     * @param values the array elements
     * @since 0.4.1
     */
    public static void getArray(MethodVisitor method, Type elementType, Object[] values) {
        getInt(method, values.length);
        method.visitTypeInsn(Opcodes.ANEWARRAY, elementType.getInternalName());
        for (int index = 0; index < values.length; index++) {
            method.visitInsn(Opcodes.DUP);
            getInt(method, index);
            getConst(method, values[index]);
            method.visitInsn(Opcodes.AASTORE);
        }
    }

    /**
     * Adds a value list on to the top of the stack.
     * @param method the current method visitor
     * @param values the array elements
     */
    public static void getList(MethodVisitor method, Collection<?> values) {
        getInt(method, values.size());
        method.visitTypeInsn(Opcodes.ANEWARRAY, typeOf(Object.class).getInternalName());
        int index = 0;
        for (Object value : values) {
            method.visitInsn(Opcodes.DUP);
            getInt(method, index++);
            getConst(method, value);
            method.visitInsn(Opcodes.AASTORE);
        }
        method.visitMethodInsn(Opcodes.INVOKESTATIC,
                typeOf(Arrays.class).getInternalName(),
                "asList",
                Type.getMethodDescriptor(typeOf(List.class), typeOf(Object[].class)),
                false);
    }

    /**
     * Adds a constant value on to the top of the stack.
     * @param method the current method visitor
     * @param value the target value
     */
    public static void getInt(MethodVisitor method, int value) {
        if (-1 <= value && value <= 5) {
            method.visitInsn(Opcodes.ICONST_0 + value);
        } else if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
            method.visitIntInsn(Opcodes.BIPUSH, value);
        } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
            method.visitIntInsn(Opcodes.SIPUSH, value);
        } else {
            method.visitLdcInsn(value);
        }
    }

    /**
     * Adds a constant value on to the top of the stack.
     * @param method the current method visitor
     * @param value the target value
     */
    public static void getLong(MethodVisitor method, long value) {
        if (0L <= value && value <= 1L) {
            method.visitInsn((int) (Opcodes.LCONST_0 + value));
        } else {
            method.visitLdcInsn(value);
        }
    }

    /**
     * Adds a constant value on to the top of the stack.
     * @param method the current method visitor
     * @param value the target value
     */
    public static void getFloat(MethodVisitor method, float value) {
        if (value == 0f || value == 1f || value == 2f) {
            method.visitInsn(Opcodes.FCONST_0 + (int) value);
        } else {
            method.visitLdcInsn(value);
        }
    }

    /**
     * Adds a constant value on to the top of the stack.
     * @param method the current method visitor
     * @param value the target value
     */
    public static void getDouble(MethodVisitor method, double value) {
        if (value == 0d || value == 1d) {
            method.visitInsn(Opcodes.DCONST_0 + (int) value);
        } else {
            method.visitLdcInsn(value);
        }
    }

    /**
     * Adds an new instance of the target type on to the top of the stack.
     * @param method the current method visitor
     * @param type the target type
     */
    public static void getNew(MethodVisitor method, TypeDescription type) {
        Type t = typeOf(type);
        method.visitTypeInsn(Opcodes.NEW, t.getInternalName());
        method.visitInsn(Opcodes.DUP);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                t.getInternalName(),
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE),
                false);
    }

    /**
     * Adds a {@code get*Option()} method invocation.
     * @param method the target method
     * @param property the target property
     */
    public static void getOption(MethodVisitor method, PropertyReference property) {
        MethodDescription decl = property.getDeclaration();
        assert decl.getParameterTypes().isEmpty();
        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                decl.getDeclaringClass().getInternalName(),
                decl.getName(),
                Type.getMethodDescriptor(typeOf(property.getType())), false);
    }

    /**
     * Copies a {@code ValueOption}.
     * @param method the target method
     * @param dataType the data type
     */
    public static void copyOption(MethodVisitor method, TypeDescription dataType) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(dataType).getInternalName(), "copyFrom",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(dataType)),
                false);
    }

    /**
     * Sets {@code null} to a {@code ValueOption}.
     * @param method the target method
     */
    public static void setNullOption(MethodVisitor method) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                VALUE_OPTION_TYPE.getInternalName(), "setNull",
                Type.getMethodDescriptor(typeOf(VALUE_OPTION_TYPE)),
                false);
    }

    /**
     * Copies a {@code DataModel}.
     * @param method the target method
     * @param dataType the data type
     */
    public static void copyDataModel(MethodVisitor method, TypeDescription dataType) {
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(dataType).getInternalName(), "copyFrom",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(dataType)),
                false);
    }

    /**
     * Adds enum constant access.
     * @param method the target method
     * @param value the target enum constant
     */
    public static void getEnumConstant(MethodVisitor method, Enum<?> value) {
        Type type = typeOf(value.getDeclaringClass());
        method.visitFieldInsn(
                Opcodes.GETSTATIC,
                type.getInternalName(),
                value.name(),
                typeOf(value.getDeclaringClass()).getDescriptor());
    }

    /**
     * Adds enum constant access.
     * @param method the target method
     * @param value the target enum constant
     */
    public static void getEnumConstant(MethodVisitor method, EnumConstantDescription value) {
        Type type = typeOf(value.getDeclaringClass());
        method.visitFieldInsn(
                Opcodes.GETSTATIC,
                type.getInternalName(),
                value.getName(),
                typeOf(value.getDeclaringClass()).getDescriptor());
    }

    /**
     * Adds {@link Result#add(Object)} method invocation.
     * @param method the current method visitor
     */
    public static void invokeResultAdd(MethodVisitor method) {
        method.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                typeOf(RESULT_TYPE).getInternalName(),
                "add",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class)),
                true);
    }

    /**
     * Adds {@link Result#add(Object)} method.
     * @param writer the current writer
     * @param callback the callback
     */
    public static void defineResultAdd(ClassVisitor writer, Consumer<MethodVisitor> callback) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "add",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class)),
                null,
                null);
        callback.accept(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds constant {@code toString()} method.
     * @param writer the current writer
     * @param value the constant value
     */
    public static void defineToString(ClassVisitor writer, String value) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "toString",
                Type.getMethodDescriptor(typeOf(String.class)),
                null,
                null);
        getConst(method, value);
        method.visitInsn(Opcodes.ARETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }

    /**
     * Adds a constructor with dependencies.
     * @param aClass the target class
     * @param writer the current writer
     * @param dependencies the target dependencies
     * @param callback the callback for building the extra constructor statements
     * @return the dependency map
     */
    public static Map<VertexElement, FieldRef> defineDependenciesConstructor(
            ClassDescription aClass,
            ClassVisitor writer,
            Iterable<? extends VertexElement> dependencies,
            Consumer<MethodVisitor> callback) {
        return defineDependenciesConstructor(aClass, writer, dependencies, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    typeOf(Object.class).getInternalName(),
                    CONSTRUCTOR_NAME,
                    Type.getMethodDescriptor(Type.VOID_TYPE),
                    false);
        }, callback);
    }

    /**
     * Adds a constructor with dependencies.
     * @param aClass the target class
     * @param writer the current writer
     * @param dependencies the target dependencies
     * @param superConstructor super constructor invocation
     * @param callback the callback for building the extra constructor statements
     * @return the dependency map
     */
    public static Map<VertexElement, FieldRef> defineDependenciesConstructor(
            ClassDescription aClass,
            ClassVisitor writer,
            Iterable<? extends VertexElement> dependencies,
            Consumer<MethodVisitor> superConstructor,
            Consumer<MethodVisitor> callback) {
        Map<VertexElement, FieldRef> results = new LinkedHashMap<>();
        int index = 0;
        int varIndex = 1;
        List<Type> parameterTypes = new ArrayList<>();
        List<Consumer<MethodVisitor>> ctor = new ArrayList<>();
        for (VertexElement element : dependencies) {
            int current = index++;
            int currentVar = varIndex;
            String name = getDependencyId(current);
            Type type = typeOf(element.getRuntimeType());
            parameterTypes.add(type);
            FieldRef ref = defineField(writer, aClass, name, type);
            ctor.add(v -> {
                v.visitVarInsn(Opcodes.ALOAD, 0);
                v.visitVarInsn(loadOpcodeOf(element.getRuntimeType()), currentVar);
                putField(v, ref);
            });
            varIndex += categoryOf(element.getRuntimeType());
            results.put(element, ref);
        }
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                CONSTRUCTOR_NAME,
                Type.getMethodDescriptor(Type.VOID_TYPE, parameterTypes.stream().toArray(Type[]::new)),
                null,
                null);
        superConstructor.accept(method);
        ctor.forEach(c -> c.accept(method));
        callback.accept(method);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
        return results;
    }

    /**
     * Returns the ID for the target dependency.
     * @param index the dependency index
     * @return the ID
     */
    public static String getDependencyId(int index) {
        return String.format("dep%d", index); //$NON-NLS-1$
    }

    /**
     * Defines a new field.
     * @param writer the current class visitor
     * @param target the declaring type
     * @param name the field name
     * @param type the field type
     * @return the defined field ref
     */
    public static FieldRef defineField(ClassVisitor writer, ClassDescription target, String name, Type type) {
        writer.visitField(
                Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
                name,
                type.getDescriptor(),
                null,
                null);
        return new FieldRef(target, name, type);
    }

    /**
     * Performs {@code PUTFIELD} instruction.
     * @param method the target method
     * @param field the target field ref
     */
    public static void putField(MethodVisitor method, FieldRef field) {
        method.visitFieldInsn(
                Opcodes.PUTFIELD,
                field.declaring.getInternalName(),
                field.name,
                field.type.getDescriptor());
    }

    /**
     * Performs {@code GETFIELD} instruction.
     * @param method the target method
     * @param field the target field ref
     */
    public static void getField(MethodVisitor method, FieldRef field) {
        method.visitFieldInsn(
                Opcodes.GETFIELD,
                field.declaring.getInternalName(),
                field.name,
                field.type.getDescriptor());
    }

    /**
     * Performs {@code GETFIELD} instruction.
     * @param method the target method
     * @param declaring the field declaring class
     * @param name the field name
     * @param type the field type
     * @return the target field
     */
    public static FieldRef getField(MethodVisitor method, ClassDescription declaring, String name, Type type) {
        FieldRef ref = new FieldRef(declaring, name, type);
        getField(method, ref);
        return ref;
    }

    /**
     * Performs {@code T_STORE} instruction.
     * @param method the target method
     * @param sort the target type sort
     * @param index the target local variable index
     * @return the target local variable ref
     */
    public static LocalVarRef putLocalVar(MethodVisitor method, int sort, int index) {
        int load;
        int store;
        switch (sort) {
        case Type.BOOLEAN:
        case Type.BYTE:
        case Type.SHORT:
        case Type.CHAR:
        case Type.INT:
            load = Opcodes.ILOAD;
            store = Opcodes.ISTORE;
            break;
        case Type.LONG:
            load = Opcodes.LLOAD;
            store = Opcodes.LSTORE;
            break;
        case Type.FLOAT:
            load = Opcodes.FLOAD;
            store = Opcodes.FSTORE;
            break;
        case Type.DOUBLE:
            load = Opcodes.DLOAD;
            store = Opcodes.DSTORE;
            break;
        case Type.ARRAY:
        case Type.OBJECT:
            load = Opcodes.ALOAD;
            store = Opcodes.ASTORE;
            break;
        default:
            throw new AssertionError(sort);
        }
        method.visitVarInsn(store, index);
        return new LocalVarRef(load, index);
    }

    /**
     * Cast the local variable.
     * @param method the writer
     * @param index the variable index
     * @param type the target type
     * @return the target variable
     */
    public static LocalVarRef cast(MethodVisitor method, int index, TypeDescription type) {
        method.visitVarInsn(Opcodes.ALOAD, index);
        method.visitTypeInsn(Opcodes.CHECKCAST, AsmUtil.typeOf(type).getInternalName());
        return putLocalVar(method, Type.OBJECT, index);
    }

    /**
     * Represents a value.
     */
    @FunctionalInterface
    public interface ValueRef {

        /**
         * Loads this variable onto top of the stack.
         * @param method the target method
         */
        void load(MethodVisitor method);
    }

    /**
     * Represents a local var.
     */
    public static class LocalVarRef implements ValueRef {

        final int opcode;

        final int operand;

        /**
         * Creates a new instance.
         * @param opcode opcode
         * @param operand operand
         */
        public LocalVarRef(int opcode, int operand) {
            this.opcode = opcode;
            this.operand = operand;
        }

        @Override
        public void load(MethodVisitor method) {
            method.visitVarInsn(opcode, operand);
        }
    }

    /**
     * Represents a field.
     */
    public static class FieldRef implements ValueRef {

        final ClassDescription declaring;

        final String name;

        final Type type;

        FieldRef(ClassDescription declaring, String name, Type type) {
            this.declaring = declaring;
            this.name = name;
            this.type = type;
        }

        @Override
        public void load(MethodVisitor method) {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, this);
        }

        /**
         * Returns the type.
         * @return the type
         */
        public Type getType() {
            return type;
        }
    }
}
