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
package com.asakusafw.lang.compiler.api.testing;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.List;

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
import com.asakusafw.runtime.value.ValueOption;

/**
 * Mock implementation of {@link DataModelProcessor}.
 * <p>
 * This accepts each {@link DataModel} implementation, which provides {@code get<property-name>Option()} methods.
 * </p>
 */
public class MockDataModelProcessor implements DataModelProcessor {

    @Override
    public boolean isSupported(Context context, TypeDescription type) {
        return resolve(context.getClassLoader(), type) != null;
    }

    @Override
    public DataModelReference process(Context context, TypeDescription type) {
        return process(context.getClassLoader(), type);
    }

    private static Class<?> resolve(ClassLoader classLoader, TypeDescription type) {
        if (type.getTypeKind() == TypeKind.CLASS) {
            Class<?> aClass;
            try {
                aClass = ((ClassDescription) type).resolve(classLoader);
            } catch (ClassNotFoundException e) {
                return null;
            }
            if (DataModel.class.isAssignableFrom(aClass)) {
                return aClass.asSubclass(DataModel.class);
            }
        }
        return null;
    }

    static DataModelReference process(ClassLoader classLoader, TypeDescription type) {
        Class<?> aClass = resolve(classLoader, type);
        if (aClass == null) {
            throw new DiagnosticException(
                    Diagnostic.Level.ERROR,
                    MessageFormat.format(
                            "unsupported data model: {0}",
                            type));
        }
        BasicDataModelReference.Builder builder = BasicDataModelReference.builder(Descriptions.classOf(aClass));
        for (Method method : aClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getParameterTypes().length != 0) {
                continue;
            }
            Class<?> valueType = method.getReturnType();
            if (ValueOption.class.isAssignableFrom(valueType) == false) {
                continue;
            }
            List<String> name = PropertyName.of(method.getName()).getWords();
            if (name.size() < 3) {
                continue;
            }
            String first = name.get(0);
            String last = name.get(name.size() - 1);
            if (first.equals("get") == false || last.equals("option") == false) { //$NON-NLS-1$ //$NON-NLS-2$
                continue;
            }
            PropertyName pName = new PropertyName(name.subList(1, name.size() - 1));
            builder.property(pName, Descriptions.typeOf(valueType), MethodDescription.of(method));
        }
        return builder.build();
    }
}
