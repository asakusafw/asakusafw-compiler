package com.asakusafw.lang.compiler.analyzer.util;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.MasterCheck;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Utilities for {@code MasterJoin} operator kind.
 */
public final class MasterJoinOperatorUtil {

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(Descriptions.classOf(MasterJoin.class));
        set.add(Descriptions.classOf(MasterCheck.class));
        set.add(Descriptions.classOf(MasterBranch.class));
        set.add(Descriptions.classOf(MasterJoinUpdate.class));
        SUPPORTED = set;
    }

    /**
     * Returns whether the target operator is {@code MasterJoin} kind or not.
     * @param operator the target operator
     * @return {@code true} if the target operator is {@code Branch} kind, otherwise {@code false}
     */
    public static boolean isSupported(Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.USER) {
            return false;
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        return SUPPORTED.contains(annotation.getDeclaringClass());
    }

    /**
     * Extracts log level in {@code Logging} operator.
     * @param classLoader the class loader to resolve target operator
     * @param operator the target branch kind operator
     * @return the log level
     * @throws ReflectiveOperationException if failed to resolve operators
     * @throws IllegalArgumentException if the target operator is not supported
     * @see #isSupported(Operator)
     */
    public static Method getSelection(
            ClassLoader classLoader,
            Operator operator) throws ReflectiveOperationException {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be a kind of MasterJoin: {0}",
                    operator));
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        ValueDescription selectionName = annotation.getElements().get(MasterSelection.ELEMENT_NAME);
        if (selectionName == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "missing master selection method name: @{1}({2}) {0}",
                    op.getMethod(),
                    annotation.getDeclaringClass().getSimpleName(),
                    MasterSelection.ELEMENT_NAME));
        }
        Object name = selectionName.resolve(classLoader);
        if ((name instanceof String) == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "inconsistent master selection method name: {3} (@{1}({2}) {0})",
                    op.getMethod(),
                    annotation.getDeclaringClass().getSimpleName(),
                    MasterSelection.ELEMENT_NAME,
                    name));
        }
        if (name.equals(MasterSelection.NO_SELECTION)) {
            return null;
        }
        Class<?> operatorClass = op.getMethod().getDeclaringClass().resolve(classLoader);
        String s = (String) name;
        for (Method m : operatorClass.getMethods()) {
            if (m.getName().equals(s) == false) {
                continue;
            }
            if (m.isAnnotationPresent(MasterSelection.class)) {
                return m;
            }
        }
        throw new NoSuchMethodException(MessageFormat.format(
                "missing master selection target method: {0}#{1} -> {2}",
                operatorClass.getName(),
                op.getMethod().getName(),
                s));
    }
}
