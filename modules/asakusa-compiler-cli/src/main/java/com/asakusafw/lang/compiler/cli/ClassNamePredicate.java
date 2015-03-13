package com.asakusafw.lang.compiler.cli;

import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.common.Predicate;

/**
 * Predicate of class names.
 */
public class ClassNamePredicate implements Predicate<Class<?>> {

    private final Pattern pattern;

    /**
     * Creates a new instance.
     * @param pattern the regex pattern
     */
    public ClassNamePredicate(Pattern pattern) {
        this.pattern = pattern;
    }

    /**
     * Creates a new instance.
     * @param pattern the class name pattern ({@code "*"} as a wildcard)
     * @return the created instance
     */
    public static ClassNamePredicate parse(String pattern) {
        Pattern regex = toRegex(pattern);
        return new ClassNamePredicate(regex);
    }

    private static Pattern toRegex(String pattern) {
        StringBuilder buf = new StringBuilder();
        int start = 0;
        while (true) {
            int next = pattern.indexOf('*', start);
            if (next < 0) {
                break;
            }
            buf.append(Pattern.quote(pattern.substring(start, next)));
            buf.append(".*"); //$NON-NLS-1$
            start = next + 1;
        }
        if (start < pattern.length()) {
            buf.append(Pattern.quote(pattern.substring(start)));
        }
        return Pattern.compile(buf.toString());
    }

    @Override
    public boolean apply(Class<?> argument) {
        String name = argument.getName();
        return pattern.matcher(name).matches();
    }
}
