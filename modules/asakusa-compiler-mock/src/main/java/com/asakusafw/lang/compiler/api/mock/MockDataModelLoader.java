package com.asakusafw.lang.compiler.api.mock;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.DataModelLoader;
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
 * Mock implementation of {@link DataModelLoader}.
 */
public class MockDataModelLoader implements DataModelLoader {

    private final Map<Class<?>, DataModelReference> cache = new HashMap<>();

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the original class loader
     */
    public MockDataModelLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public DataModelReference load(TypeDescription type) {
        try {
            if (type.getTypeKind() == TypeKind.CLASS) {
                Class<?> aClass = ((ClassDescription) type).resolve(classLoader);
                DataModelReference cached = cache.get(aClass);
                if (cached != null) {
                    return cached;
                }
                if (DataModel.class.isAssignableFrom(aClass)) {
                    DataModelReference result = load0(aClass);
                    if (result != null) {
                        cache.put(aClass, result);
                        return result;
                    }
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(
                    Diagnostic.Level.ERROR,
                    MessageFormat.format(
                            "failed to load data model: {0}",
                            type),
                    e);
        }
        throw new DiagnosticException(
                Diagnostic.Level.ERROR,
                MessageFormat.format(
                        "unsupported data model: {0}",
                        type));
    }

    private DataModelReference load0(Class<?> aClass) {
        BasicDataModelReference.Builder builder = BasicDataModelReference.builder(Descriptions.classOf(aClass));
        for (Method method : aClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getParameterTypes().length != 0) {
                continue;
            }
            Class<?> type = method.getReturnType();
            if (ValueOption.class.isAssignableFrom(type) == false) {
                continue;
            }
            List<String> name = PropertyName.of(method.getName()).getWords();
            if (name.size() < 3) {
                continue;
            }
            if (name.get(0).equals("get") == false || name.get(name.size() - 1).equals("option") == false) {
                continue;
            }
            PropertyName pName = new PropertyName(name.subList(1, name.size() - 1));
            builder.property(pName, Descriptions.typeOf(type), MethodDescription.of(method));
        }
        return builder.build();
    }
}
