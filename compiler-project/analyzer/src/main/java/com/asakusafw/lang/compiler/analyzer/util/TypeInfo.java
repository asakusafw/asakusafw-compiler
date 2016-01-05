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
package com.asakusafw.lang.compiler.analyzer.util;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Type utilities.
 */
public final class TypeInfo {

    private final Class<?> rawType;

    private final List<Type> typeArguments;

    private TypeInfo(Class<?> raw, List<? extends Type> arguments) {
        this.rawType = raw;
        this.typeArguments = new ArrayList<>(arguments);
    }

    /**
     * Returns whether the target type is class or interface type or not.
     * @param type the target type
     * @return {@code true} if the target type is a class or interface type
     */
    public static boolean isClassOrInterface(Type type) {
        if (type instanceof Class<?>) {
            Class<?> aClass = (Class<?>) type;
            return aClass.isArray() == false && aClass.isPrimitive() == false;
        }
        return type instanceof ParameterizedType;
    }

    /**
     * Creates a new instance.
     * @param type the original type (must be a class or interface type)
     * @return the type information
     */
    public static TypeInfo of(Type type) {
        if (type instanceof Class<?> == false && isClassOrInterface(type) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "type must be a class or interface type: {0}",
                    type));
        }
        GenericContext generic = toGenericContext(type);
        assert generic != null : "type must be a class or interface type";
        return new TypeInfo(generic.raw, new ArrayList<>(generic.mapping.values()));
    }

    /**
     * Creates a new instance.
     * @param rawType the raw type
     * @param typeArguments the type arguments
     * @return the type information
     */
    public static TypeInfo of(Class<?> rawType, Type... typeArguments) {
        return new TypeInfo(rawType, Arrays.asList(typeArguments));
    }

    /**
     * Creates a new instance.
     * @param rawType the raw type
     * @param typeArguments the type arguments
     * @return the type information
     */
    public static TypeInfo of(Class<?> rawType, List<? extends Type> typeArguments) {
        return new TypeInfo(rawType, typeArguments);
    }

    /**
     * Returns the raw type.
     * @return the raw type
     */
    public Class<?> getRawType() {
        return rawType;
    }

    /**
     * Returns the type arguments.
     * @return the type arguments
     */
    public List<Type> getTypeArguments() {
        return typeArguments;
    }

    /**
     * Returns the erased type arguments.
     * @return the erased type arguments
     */
    public List<Class<?>> getErasedTypeArguments() {
        List<Class<?>> results = new ArrayList<>();
        for (Type t : getTypeArguments()) {
            results.add(erase(t));
        }
        return results;
    }

    /**
     * Returns this type as {@code java.lang.reflect.Type}.
     * @return the type
     */
    public Type toType() {
        if (typeArguments.isEmpty()) {
            return rawType;
        } else {
            return new SimpleParameterizedType(rawType, typeArguments);
        }
    }

    @Override
    public String toString() {
        if (typeArguments.isEmpty()) {
            return rawType.getName();
        }
        StringBuilder buf = new StringBuilder();
        buf.append(rawType.getName());
        buf.append('<');
        for (Class<?> c : getErasedTypeArguments()) {
            buf.append(c.getName());
            buf.append(", "); //$NON-NLS-1$
        }
        buf.delete(buf.length() - 2, buf.length());
        buf.append('>');
        return buf.toString();
    }

    /**
     * Returns the erasure type.
     * @param type the target type
     * @return the related erasure type
     */
    public static Class<?> erase(Type type) {
        if (type instanceof TypeVariable<?>) {
            Type[] bounds = ((TypeVariable<?>) type).getBounds();
            if (bounds == null || bounds.length == 0) {
                return Object.class;
            }
            return erase(bounds[0]);
        } else if (type instanceof GenericArrayType) {
            Class<?> component = erase(((GenericArrayType) type).getGenericComponentType());
            Object array = Array.newInstance(component, 0);
            return array.getClass();
        } else {
            GenericContext generic = toGenericContext(type);
            assert generic != null : "type must be a class or interface type";
            return generic.raw;
        }
    }

    /**
     * Returns the invocation type.
     * @param target the target type
     * @param context the context type
     * @return the invoked type
     */
    public static List<Type> invoke(Class<?> target, Type context) {
        if (target == Object.class) {
            return Collections.emptyList();
        }

        GenericContext generic = toGenericContext(context);
        assert generic != null : "context must be a class or interface type";
        if (target.isAssignableFrom(generic.raw) == false) {
            return null;
        }
        if (target.getTypeParameters().length == 0) {
            return Collections.emptyList();
        }

        if (target.isInterface()) {
            return invokeInterface(target, generic);
        }
        if (generic.raw.isInterface()) {
            return null;
        }
        return invokeClass(target, generic);
    }

    private static List<Type> invokeClass(Class<?> target, GenericContext context) {
        assert target != null;
        assert context != null;
        assert target.isInterface() == false;
        assert context.raw.isInterface() == false;
        for (GenericContext current = context.getSuperClass();
                current != null;
                current = current.getSuperClass()) {
            if (current.raw == target) {
                return current.getTypeArguments();
            }
        }
        return null;
    }

    private static List<Type> invokeInterface(Class<?> target, GenericContext context) {
        assert target != null;
        assert context != null;
        assert target.isInterface();
        GenericContext bottom = findBottomClass(target, context);
        if (bottom == null) {
            return null;
        } else if (target == bottom.raw) {
            return bottom.getTypeArguments();
        }
        return findInterface(target, bottom);
    }

    private static List<Type> findInterface(
            Class<?> target,
            GenericContext context) {
        assert target != null;
        assert context != null;
        assert target.isAssignableFrom(context.raw);
        for (Iterator<GenericContext> iter = context.getSuperInterfaces(); iter.hasNext();) {
            GenericContext intf = iter.next();
            if (target == intf.raw) {
                return intf.getTypeArguments();
            }
            if (target.isAssignableFrom(intf.raw)) {
                return findInterface(target, intf);
            }
        }
        throw new AssertionError(target);
    }

    private static GenericContext findBottomClass(
            Class<?> target,
            GenericContext context) {
        assert target != null;
        assert context != null;
        GenericContext bottom = null;
        for (GenericContext current = context; current != null; current = current.getSuperClass()) {
            if (target.isAssignableFrom(current.raw)) {
                bottom = current;
            } else {
                break;
            }
        }
        return bottom;
    }

    private static GenericContext toGenericContext(Type context) {
        assert context != null;
        if (context instanceof Class<?>) {
            return new GenericContext((Class<?>) context);
        } else if (context instanceof ParameterizedType) {
            ParameterizedType t = (ParameterizedType) context;
            Class<?> raw = (Class<?>) t.getRawType();
            TypeVariable<?>[] params = raw.getTypeParameters();
            Type[] args = t.getActualTypeArguments();
            if (params.length != args.length) {
                return new GenericContext(raw);
            }
            LinkedHashMap<TypeVariable<?>, Type> mapping = new LinkedHashMap<>();
            for (int i = 0; i < params.length; i++) {
                mapping.put(params[i], args[i]);
            }
            return new GenericContext(raw, mapping);
        } else if (context instanceof WildcardType) {
            WildcardType wild = (WildcardType) context;
            Type[] upper = wild.getUpperBounds();
            if (upper.length == 0) {
                return new GenericContext(Object.class);
            } else {
                return toGenericContext(upper[0]);
            }
        }
        return null;
    }

    static GenericContext analyze(Type type, Map<TypeVariable<?>, Type> mapping) {
        assert type != null;
        assert mapping != null;
        Type subst = substitute(type, mapping);
        if (subst == null) {
            return null;
        }
        return toGenericContext(subst);
    }

    private static Type substitute(Type type, Map<TypeVariable<?>, Type> mapping) {
        assert type != null;
        assert mapping != null;
        if (type instanceof Class<?>) {
            return type;
        } else if (type instanceof TypeVariable<?>) {
            return mapping.get(type);
        } else if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Class<?> raw = (Class<?>) pt.getRawType();
            List<Type> arguments = new ArrayList<>();
            for (Type t : pt.getActualTypeArguments()) {
                Type subst = substitute(t, mapping);
                if (subst == null) {
                    return raw;
                }
                arguments.add(subst);
            }
            return new SimpleParameterizedType(raw, arguments);
        }
        return null;
    }

    private static class GenericContext {

        final Class<?> raw;

        final LinkedHashMap<TypeVariable<?>, Type> mapping;

        GenericContext(Class<?> raw) {
            this.raw = raw;
            this.mapping = new LinkedHashMap<>();
        }

        GenericContext(Class<?> raw, LinkedHashMap<TypeVariable<?>, Type> mapping) {
            this.raw = raw;
            this.mapping = mapping;
        }

        public List<Type> getTypeArguments() {
            return new ArrayList<>(mapping.values());
        }

        public GenericContext getSuperClass() {
            if (raw.getSuperclass() == null) {
                return null;
            }
            Type parent = raw.getGenericSuperclass();
            return analyze(parent, mapping);
        }

        public Iterator<GenericContext> getSuperInterfaces() {
            return new Iterator<TypeInfo.GenericContext>() {
                Type[] interfaces = raw.getGenericInterfaces();
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < interfaces.length;
                }

                @Override
                public GenericContext next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return analyze(interfaces[index++], mapping);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    private static class SimpleParameterizedType implements ParameterizedType {

        private final Class<?> rawType;

        private final List<Type> typeArguments;

        SimpleParameterizedType(
                Class<?> rawType,
                List<Type> typeArguments) {
            this.rawType = rawType;
            this.typeArguments = typeArguments;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return typeArguments.toArray(new Type[typeArguments.size()]);
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            return rawType.getDeclaringClass();
        }
    }
}
