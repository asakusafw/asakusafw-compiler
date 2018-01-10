/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.basic;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.basic.BasicDataModelReference;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.model.PropertyOrder;
import com.asakusafw.runtime.value.ValueOption;

/**
 * A basic implementation of {@link DataModelProcessor}.
 */
public class BasicDataModelProcessor implements DataModelProcessor {

    static final Logger LOG = LoggerFactory.getLogger(BasicDataModelProcessor.class);

    private Class<?> resolve(Context context, TypeDescription type) {
        if (type.getTypeKind() == TypeKind.CLASS) {
            Class<?> aClass;
            try {
                aClass = ((ClassDescription) type).resolve(context.getClassLoader());
            } catch (ClassNotFoundException e) {
                return null;
            }
            if (DataModel.class.isAssignableFrom(aClass)) {
                return aClass.asSubclass(DataModel.class);
            }
        }
        return null;
    }

    @Override
    public boolean isSupported(Context context, TypeDescription type) {
        return resolve(context, type) != null;
    }

    @Override
    public DataModelReference process(Context context, TypeDescription type) {
        Class<?> aClass = resolve(context, type);
        if (aClass == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "unsupported data model: {0}",
                    type));
        }
        Map<PropertyName, Method> map = extractPropertyMap(aClass);
        List<PropertyName> order = extractPropertyOrder(aClass, map);
        BasicDataModelReference.Builder builder = BasicDataModelReference.builder(Descriptions.classOf(aClass));
        for (PropertyName name : order) {
            Method method = map.get(name);
            assert method != null;
            builder.property(name, Descriptions.typeOf(method.getReturnType()), MethodDescription.of(method));
        }
        return builder.build();
    }

    private Map<PropertyName, Method> extractPropertyMap(Class<?> aClass) {
        Map<PropertyName, Method> results = new LinkedHashMap<>();
        for (Method method : aClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getParameterTypes().length != 0) {
                continue;
            }
            Class<?> type = method.getReturnType();
            if (ValueOption.class.isAssignableFrom(type) == false || type == ValueOption.class) {
                continue;
            }
            List<String> name = PropertyName.of(method.getName()).getWords();
            if (name.size() < 3) {
                continue;
            }
            if (name.get(0).equals("get") == false //$NON-NLS-1$
                    || name.get(name.size() - 1).equals("option") == false) { //$NON-NLS-1$
                continue;
            }
            PropertyName pName = new PropertyName(name.subList(1, name.size() - 1));
            results.put(pName, method);
        }
        return results;
    }

    private List<PropertyName> extractPropertyOrder(Class<?> aClass, Map<PropertyName, Method> methods) {
        PropertyOrder order = aClass.getAnnotation(PropertyOrder.class);
        if (order == null) {
            return new ArrayList<>(methods.keySet());
        }
        Set<PropertyName> saw = new HashSet<>();
        List<PropertyName> results = new ArrayList<>();
        for (String s : order.value()) {
            PropertyName name = PropertyName.of(s);
            if (methods.containsKey(name)) {
                results.add(name);
                saw.add(name);
            } else {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "missing data model property: {0}#{1}",
                        aClass.getName(),
                        name.toName()));
            }
        }
        for (PropertyName name : methods.keySet()) {
            if (saw.contains(name) == false) {
                results.add(name);
                saw.add(name);
            }
        }
        return results;
    }
}
