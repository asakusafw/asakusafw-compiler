package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * An argument on operators.
 */
public class OperatorArgument implements OperatorProperty {

    private final String name;

    private final ValueDescription value;

    /**
     * Creates a new instance.
     * @param name the argument name
     * @param value the argument value
     */
    public OperatorArgument(String name, ValueDescription value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public PropertyKind getPropertyKind() {
        return PropertyKind.ARGUMENT;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the value of this argument.
     * @return the value
     */
    public ValueDescription getValue() {
        return value;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Argument(name={0}, value={1})",
                name,
                value);
    }
}
