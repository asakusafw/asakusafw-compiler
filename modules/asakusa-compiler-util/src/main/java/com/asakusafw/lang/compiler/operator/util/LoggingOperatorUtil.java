package com.asakusafw.lang.compiler.operator.util;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.Logging;

/**
 * Utilities for {@code Logging} like operators.
 */
public final class LoggingOperatorUtil {

    private static final String NAME_ELEMENT = "value"; //$NON-NLS-1$

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(Descriptions.classOf(Logging.class));
        SUPPORTED = set;
    }

    private LoggingOperatorUtil() {
        return;
    }

    /**
     * Returns whether the target operator is {@code Logging} kind or not.
     * @param operator the target operator
     * @return {@code true} if the target operator is {@code Logging} kind, otherwise {@code false}
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
     */
    public static Logging.Level getLogLevel(
            ClassLoader classLoader,
            Operator operator) throws ReflectiveOperationException {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be a kind of Logging: {0}",
                    operator));
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        ValueDescription level = annotation.getElements().get(NAME_ELEMENT);
        if (level == null || level.getValueKind() != ValueKind.ENUM_CONSTANT) {
            throw new IllegalStateException(MessageFormat.format(
                    "Logging kind operator must have log level: {0}",
                    op.getMethod()));
        }
        Object resolved = ((EnumConstantDescription) level).resolve(classLoader);
        if ((resolved instanceof Logging.Level) == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "log level of Logging kind operator must be instance of Logging.Level: {0}",
                    resolved));
        }
        return (Logging.Level) resolved;
    }
}
