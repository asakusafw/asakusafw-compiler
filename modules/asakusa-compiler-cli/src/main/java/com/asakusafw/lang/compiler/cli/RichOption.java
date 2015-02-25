package com.asakusafw.lang.compiler.cli;

import org.apache.commons.cli.Option;

/**
 * A wrapped option.
 */
public final class RichOption extends Option {

    private static final long serialVersionUID = 3472131811098458132L;

    /**
     * Creates a new instance.
     * @param shortName the short name
     * @param longName the long name
     * @param numerOfArguments the number of arguments
     * @param required whether this option is required or not
     */
    public RichOption(String shortName, String longName, int numerOfArguments, boolean required) {
        super(shortName, longName, false, null);
        setArgs(numerOfArguments);
        setRequired(required);
    }

    /**
     * Sets the option description.
     * @param value the description
     * @return this
     */
    public RichOption withDescription(String value) {
        setDescription(value);
        return this;
    }

    /**
     * Sets the argument description.
     * @param value the description
     * @return this
     */
    public RichOption withArgumentDescription(String value) {
        setArgName(value);
        return this;
    }
}
