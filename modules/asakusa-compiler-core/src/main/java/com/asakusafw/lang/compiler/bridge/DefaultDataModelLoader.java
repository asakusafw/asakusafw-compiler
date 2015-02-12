package com.asakusafw.lang.compiler.bridge;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.Diagnostic;
import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.lang.compiler.api.basic.BasicDataModelReference;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
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
 * A default implementation of {@link DataModelLoader}.
 */
public class DefaultDataModelLoader implements DataModelLoader {

    private final Map<Class<?>, DataModelReference> cache = new HashMap<>();

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the original class loader
     */
    public DefaultDataModelLoader(ClassLoader classLoader) {
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
            if (name.get(0).equals("get") == false || name.get(name.size() - 1).equals("option") == false) {
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
                throw new DiagnosticException(
                        Diagnostic.Level.ERROR,
                        MessageFormat.format(
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
