package com.asakusafw.bridge.redirector;

import org.apache.commons.cli.Option;

/**
 * A wrapped option.
 */
public final class RichOption extends Option {

    private static final long serialVersionUID = 5475323655516362178L;

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

    /**
     * Sets the value separator.
     * @param separator the key-value separator
     * @return this
     */
    public RichOption withValueSeparator(char separator) {
        this.setValueSeparator(separator);
        return this;
    }
}
