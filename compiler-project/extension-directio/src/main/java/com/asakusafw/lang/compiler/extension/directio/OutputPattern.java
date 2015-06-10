/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.directio;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.runtime.stage.directio.StringTemplate.Format;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * Processes patterns in {@link DirectFileOutputDescription}.
 */
public final class OutputPattern {

    private static final Map<TypeDescription, Class<?>> VALUE_TYPES;
    static {
        Class<?>[] classes = {
                BooleanOption.class,
                ByteOption.class,
                ShortOption.class,
                IntOption.class,
                LongOption.class,
                FloatOption.class,
                DoubleOption.class,
                DateOption.class,
                DateTimeOption.class,
                StringOption.class,
        };
        Map<TypeDescription, Class<?>> map = new HashMap<>();
        for (Class<?> aClass : classes) {
            map.put(Descriptions.classOf(aClass), aClass);
        }
        VALUE_TYPES = Collections.unmodifiableMap(map);
    }

    static final int CHAR_BRACE_OPEN = '{';

    static final int CHAR_BRACE_CLOSE = '}';

    static final int CHAR_BLOCK_OPEN = '[';

    static final int CHAR_BLOCK_CLOSE = ']';

    static final int CHAR_WILDCARD = '*';

    static final int CHAR_SEPARATE_IN_BLOCK = ':';

    static final int CHAR_VARIABLE_START = '$';

    static final BitSet CHAR_MAP_META = new BitSet();
    static {
        CHAR_MAP_META.set(0, 0x20);
        CHAR_MAP_META.set('\\');
        CHAR_MAP_META.set('*');
        CHAR_MAP_META.set('?');
        CHAR_MAP_META.set('#');
        CHAR_MAP_META.set('|');
        CHAR_MAP_META.set('{');
        CHAR_MAP_META.set('}');
        CHAR_MAP_META.set('[');
        CHAR_MAP_META.set(']');
    }

    private final String resourcePatternString;

    private final List<CompiledSegment> resourcePattern;

    private final List<CompiledOrder> orders;

    private OutputPattern(
            String resourcePatternString,
            List<CompiledSegment> resourcePattern, List<CompiledOrder> orders) {
        this.resourcePatternString = resourcePatternString;
        this.resourcePattern = resourcePattern;
        this.orders = orders;
    }

    /**
     * Compiles the output resource pattern and their record orderings.
     * @param dataModel the target data model
     * @param resourcePattern the output path pattern
     * @param orders the output ordering
     * @return the compiled information
     */
    public static OutputPattern compile(DataModelReference dataModel, String resourcePattern, List<String> orders) {
        List<CompiledSegment> compiledPattern = compileResourcePattern(resourcePattern, dataModel);
        List<CompiledOrder> compiledOrders = compileOrder(orders, dataModel);
        return new OutputPattern(resourcePattern, compiledPattern, compiledOrders);
    }

    /**
     * Compiles the output resource pattern.
     * @param dataModel the target data model
     * @param resourcePattern the output path pattern
     * @return the compiled information
     */
    public static OutputPattern compile(DataModelReference dataModel, String resourcePattern) {
        return compile(dataModel, resourcePattern, Collections.<String>emptyList());
    }

    /**
     * Returns the string representation of output path pattern.
     * @return output path pattern
     */
    public String getResourcePatternString() {
        return resourcePatternString;
    }

    /**
     * Returns the compiled output path pattern.
     * @return output path pattern
     */
    public List<CompiledSegment> getResourcePattern() {
        return resourcePattern;
    }

    /**
     * Returns the compiled record ordering.
     * @return record ordering
     */
    public List<CompiledOrder> getOrders() {
        return orders;
    }

    /**
     * Returns whether this output pattern depends on the context information or not.
     * With it, the framework will create file per map tasks.
     * @return {@code true} if this depends on the context information, otherwise {@code false}
     */
    public boolean isContextRequired() {
        if (isContextRequired(resourcePattern)) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether this output pattern requires gathering records or not.
     * With it, this framework will create file per reduce group.
     * @return {@code true} if gather is required, otherwise {@code false}
     */
    public boolean isGatherRequired() {
        if (isGatherRequired(resourcePattern)) {
            return true;
        }
        if (orders.isEmpty() == false) {
            return true;
        }
        return false;
    }

    /**
     * Returns whether the output pattern depends on the context information or not.
     * With it, the framework will create file per map tasks.
     * @param resourcePattern the target output pattern
     * @return {@code true} if the pattern depends on the context information, otherwise {@code false}
     */
    public static boolean isContextRequired(List<CompiledSegment> resourcePattern) {
        for (CompiledSegment segment : resourcePattern) {
            switch (segment.getKind()) {
            case CONTEXT:
                return true;
            default:
                break;
            }
        }
        return false;
    }

    /**
     * Returns whether the output pattern requires gathering records or not.
     * With it, this framework will create file per reduce group.
     * @param resourcePattern the output pattern
     * @return {@code true} if gather is required for the pattern, otherwise {@code false}
     */
    public static boolean isGatherRequired(List<CompiledSegment> resourcePattern) {
        for (CompiledSegment segment : resourcePattern) {
            switch (segment.getKind()) {
            case PROPERTY:
            case RANDOM:
                return true;
            default:
                break;
            }
        }
        return isContextRequired(resourcePattern) == false;
    }

    /**
     * Compiles the resource pattern for the output.
     * @param pattern the pattern string
     * @param dataType target data type
     * @return the compiled objects
     * @throws IllegalArgumentException if pattern is invalid
     * @see DirectFileOutputDescription#getResourcePattern()
     */
    public static List<CompiledSegment> compileResourcePattern(String pattern, DataModelReference dataType) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern must not be null"); //$NON-NLS-1$
        }
        if (dataType == null) {
            throw new IllegalArgumentException("dataType must not be null"); //$NON-NLS-1$
        }
        List<CompiledSegment> results = new ArrayList<>();
        Cursor cursor = new Cursor(pattern);
        while (cursor.isEof() == false) {
            if (cursor.isLiteral()) {
                String literal = cursor.consumeLiteral();
                results.add(new CompiledSegment(literal));
            } else if (cursor.isPlaceHolder()) {
                Formatted ph = cursor.consumePlaceHolder();
                PropertyReference property = findProperty(dataType, (String) ph.original);
                if (property == null) {
                    cursor.rewind();
                    throw new IllegalArgumentException(MessageFormat.format(
                            "unknown property \"{1}\": {0}",
                            cursor,
                            ph.original));
                }
                String argument = ph.formatString;
                Format format = findFormat(property, argument);
                if (format == null) {
                    cursor.rewind();
                    throw new IllegalArgumentException(MessageFormat.format(
                            "invalid format \"{1}\": {0}",
                            cursor,
                            argument == null ? "" : argument)); //$NON-NLS-1$
                }
                try {
                    format.check(getType(property), argument);
                } catch (IllegalArgumentException e) {
                    cursor.rewind();
                    throw new IllegalArgumentException(MessageFormat.format(
                            "invalid format \"{1}\": {0}",
                            cursor,
                            argument == null ? "" : argument), e); //$NON-NLS-1$
                }
                results.add(new CompiledSegment(property, format, argument));
            } else if (cursor.isRandomNumber()) {
                Formatted rand = cursor.consumeRandomNumber();
                RandomNumber source = (RandomNumber) rand.original;
                results.add(new CompiledSegment(source, Format.NATURAL, null));
            } else if (cursor.isWildcard()) {
                cursor.consumeWildcard();
                results.add(new CompiledSegment());
            } else {
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid character: {0}",
                        cursor));
            }
        }
        return results;
    }

    /**
     * Compiled the ordering for the output.
     * @param orders the each order representation
     * @param dataType target data type
     * @return the compiled objects
     * @throws IllegalArgumentException if pattern is invalid
     * @see DirectFileOutputDescription#getOrder()
     */
    public static List<CompiledOrder> compileOrder(List<String> orders, DataModelReference dataType) {
        if (orders == null) {
            throw new IllegalArgumentException("orders must not be null"); //$NON-NLS-1$
        }
        if (dataType == null) {
            throw new IllegalArgumentException("dataType must not be null"); //$NON-NLS-1$
        }
        Set<PropertyName> saw = new HashSet<>();
        List<CompiledOrder> results = new ArrayList<>();
        for (String order : orders) {
            Group.Ordering ordering;
            try {
                ordering = Groups.parseOrder(order.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid order format: {0}",
                        order), e);
            }
            PropertyReference property = dataType.findProperty(ordering.getPropertyName());
            if (property == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "unknown property \"{1}\": {0}",
                        order,
                        ordering.getPropertyName()));
            }
            if (saw.contains(property.getName())) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "duplicate property \"{1}\": {0}",
                        order,
                        ordering.getPropertyName()));
            }
            saw.add(property.getName());
            results.add(new CompiledOrder(property, ordering.getDirection() == Group.Direction.ASCENDANT));
        }
        return results;
    }

    private static PropertyReference findProperty(DataModelReference dataType, String name) {
        assert dataType != null;
        assert name != null;
        return dataType.findProperty(PropertyName.of(name));
    }

    private static Format findFormat(PropertyReference property, String argument) {
        if (argument == null) {
            return Format.NATURAL;
        }
        Class<?> type = getType(property);
        if (type == DateOption.class) {
            return Format.DATE;
        }
        if (type == DateTimeOption.class) {
            return Format.DATETIME;
        }
        return null;
    }

    static Class<?> getType(PropertyReference property) {
        Class<?> type = VALUE_TYPES.get(property.getType());
        if (type == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "unknown property type: {0}",
                    property));
        }
        return type;
    }

    private static final class Cursor {

        private final char[] cbuf;

        private int lastSegmentPosition;

        private int position;

        Cursor(String value) {
            assert value != null;
            this.cbuf = value.toCharArray();
            this.position = 0;
        }

        boolean isEof() {
            return cbuf.length == position;
        }

        boolean isLiteral() {
            if (isEof()) {
                return false;
            }
            return CHAR_MAP_META.get(cbuf[position]) == false;
        }

        boolean isPlaceHolder() {
            if (isEof()) {
                return false;
            }
            return cbuf[position] == CHAR_BRACE_OPEN;
        }

        boolean isRandomNumber() {
            if (isEof()) {
                return false;
            }
            return cbuf[position] == CHAR_BLOCK_OPEN;
        }

        boolean isWildcard() {
            if (isEof()) {
                return false;
            }
            return cbuf[position] == CHAR_WILDCARD;
        }

        void rewind() {
            this.position = lastSegmentPosition;
        }

        String consumeLiteral() {
            assert isLiteral();
            this.lastSegmentPosition = position;
            int start = position;
            while (isLiteral()) {
                char c = cbuf[position];
                if (c == CHAR_VARIABLE_START) {
                    skipVariable();
                } else if (CHAR_MAP_META.get(c) == false) {
                    advance();
                } else {
                    throw new AssertionError(c);
                }
            }
            return String.valueOf(cbuf, start, position - start);
        }

        private void skipVariable() {
            int start = position;
            assert cbuf[position] == CHAR_VARIABLE_START;
            advance();
            if (isEof() || cbuf[position] != CHAR_BRACE_OPEN) {
                return;
            }
            advance();
            while (true) {
                if (isEof()) {
                    position = start;
                    throw new IllegalArgumentException(MessageFormat.format(
                            "variable is not closed: {0}",
                            this));
                }
                char c = cbuf[position];
                if (c == CHAR_BRACE_CLOSE) {
                    break;
                }
                advance();
            }
            advance();
        }

        Formatted consumePlaceHolder() {
            assert isPlaceHolder();
            this.lastSegmentPosition = position;
            int start = position + 1;
            String propertyName;
            String formatString;
            advance();
            while (true) {
                if (isEof()) {
                    position = start;
                    throw new IllegalArgumentException(MessageFormat.format(
                            "placeholder is not closed: {0}",
                            this));
                }
                char c = cbuf[position];
                if (c == CHAR_BRACE_CLOSE || c == CHAR_SEPARATE_IN_BLOCK) {
                    break;
                }
                advance();
            }
            propertyName = String.valueOf(cbuf, start, position - start);
            if (cbuf[position] == CHAR_SEPARATE_IN_BLOCK) {
                advance();
                int formatStart = position;
                while (true) {
                    if (isEof()) {
                        position = start;
                        throw new IllegalArgumentException(MessageFormat.format(
                                "placeholder is not closed: {0}",
                                this));
                    }
                    char c = cbuf[position];
                    if (c == CHAR_BRACE_CLOSE) {
                        break;
                    }
                    advance();
                }
                formatString = String.valueOf(cbuf, formatStart, position - formatStart);
            } else {
                formatString = null;
            }
            assert cbuf[position] == CHAR_BRACE_CLOSE;
            advance();
            return new Formatted(propertyName, formatString);
        }

        private static final Pattern RNG = Pattern.compile("(\\d+)\\.{2,3}(\\d+)(:(.*))?"); //$NON-NLS-1$
        Formatted consumeRandomNumber() {
            assert isRandomNumber();
            this.lastSegmentPosition = position;
            int start = position + 1;
            while (true) {
                if (isEof()) {
                    position = start;
                    throw new IllegalArgumentException(MessageFormat.format(
                            "random number is not closed: {0}",
                            this));
                }
                char c = cbuf[position];
                if (c == CHAR_BLOCK_CLOSE) {
                    break;
                }
                advance();
            }
            String content = String.valueOf(cbuf, start, position - start);
            Matcher matcher = RNG.matcher(content);
            if (matcher.matches() == false) {
                position = start;
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid random number format: {0}",
                        this));
            }
            int lower;
            try {
                lower = Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                position = start + matcher.start(1);
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid random number format: {0}",
                        this), e);
            }
            int upper;
            try {
                upper = Integer.parseInt(matcher.group(2));
            } catch (NumberFormatException e) {
                position = start + matcher.start(2);
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid random number format: {0}",
                        this), e);
            }
            if (lower >= upper) {
                position = start + matcher.start(1);
                throw new IllegalArgumentException(MessageFormat.format(
                        "the random number [lower..upper] must be lower < upper: {0}",
                        this));
            }

            String format = matcher.group(4);
            advance();
            return new Formatted(new RandomNumber(lower, upper), format);
        }

        void consumeWildcard() {
            assert isWildcard();
            advance();
        }

        private void advance() {
            position = Math.min(position + 1, cbuf.length);
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append('[');
            buf.append(cbuf, 0, position);
            buf.append(" >> "); //$NON-NLS-1$
            buf.append(cbuf, position, cbuf.length - position);
            buf.append(']');
            return buf.toString();
        }
    }

    private static class Formatted {

        final Object original;

        final String formatString;

        Formatted(Object original, String formatString) {
            this.original = original;
            this.formatString = formatString;
        }
    }

    /**
     * The compiled resource pattern.
     */
    public static final class CompiledSegment {

        private final SourceKind kind;

        private final Object source;

        private final Format format;

        private final String argument;

        /**
         * Creates a new wildcard.
         */
        public CompiledSegment() {
            this.kind = SourceKind.CONTEXT;
            this.source = null;
            this.format = Format.PLAIN;
            this.argument = null;
        }

        /**
         * Creates a new literal.
         * @param string literal value
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public CompiledSegment(String string) {
            if (string == null) {
                throw new IllegalArgumentException("string must not be null"); //$NON-NLS-1$
            }
            this.kind = SourceKind.NOTHING;
            this.source = null;
            this.format = Format.PLAIN;
            this.argument = string;
        }

        /**
         * Creates a new instance.
         * @param target target property
         * @param format format kind
         * @param argument format argument (nullable)
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public CompiledSegment(PropertyReference target, Format format, String argument) {
            if (target == null) {
                throw new IllegalArgumentException("target must not be null"); //$NON-NLS-1$
            }
            if (format == null) {
                throw new IllegalArgumentException("format must not be null"); //$NON-NLS-1$
            }
            this.kind = SourceKind.PROPERTY;
            this.source = target;
            this.format = format;
            this.argument = argument;
            format.check(getType(target), argument);
        }

        /**
         * Creates a new instance.
         * @param source the source object
         * @param format format kind
         * @param argument format argument (nullable)
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public CompiledSegment(RandomNumber source, Format format, String argument) {
            if (source == null) {
                throw new IllegalArgumentException("source must not be null"); //$NON-NLS-1$
            }
            if (format == null) {
                throw new IllegalArgumentException("format must not be null"); //$NON-NLS-1$
            }
            this.kind = SourceKind.RANDOM;
            this.source = source;
            this.format = format;
            this.argument = argument;
        }

        /**
         * Returns the kind of the souce of this fragment.
         * @return the kind
         */
        public SourceKind getKind() {
            return kind;
        }

        /**
         * Returns the source of this fragment.
         * @return the source, or {@code null} if the source is not specified
         */
        public Object getSource() {
            return source;
        }

        /**
         * Returns the target property.
         * @return the target property, or {@code null} if the source is not a property
         * @see #getSource()
         */
        public PropertyReference getTarget() {
            if (kind != SourceKind.PROPERTY) {
                return null;
            }
            return (PropertyReference) source;
        }

        /**
         * Returns the random number specification.
         * @return the random number spec, or {@code null} if the source is not a random number
         * @see #getSource()
         */
        public RandomNumber getRandomNumber() {
            if (kind != SourceKind.RANDOM) {
                return null;
            }
            return (RandomNumber) source;
        }

        /**
         * Returns the format kind.
         * @return the format
         */
        public Format getFormat() {
            return format;
        }

        /**
         * Returns the argument for the format.
         * @return the argument, or {@code null} if not defined
         */
        public String getArgument() {
            return argument;
        }
    }

    /**
     * The compiled ordering pattern.
     */
    public static final class CompiledOrder {

        private final PropertyReference target;

        private final boolean ascend;

        /**
         * Creates a new instance.
         * @param target target property
         * @param ascend whether the ordering is ascend
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public CompiledOrder(PropertyReference target, boolean ascend) {
            if (target == null) {
                throw new IllegalArgumentException("target must not be null"); //$NON-NLS-1$
            }
            this.target = target;
            this.ascend = ascend;
        }

        /**
         * Returns the target property.
         * @return the target property
         */
        public PropertyReference getTarget() {
            return target;
        }

        /**
         * Returns whether the ordering is ascend.
         * @return {@code true} for ascend, or {@code false} for descend
         */
        public boolean isAscend() {
            return ascend;
        }
    }

    /**
     * The source kind.
     */
    public enum SourceKind {

        /**
         * Source is nothing (for literals).
         */
        NOTHING,

        /**
         * Source is a property.
         * @see PropertyReference
         */
        PROPERTY,

        /**
         * Source is a random number generator.
         * @see RandomNumber
         */
        RANDOM,

        /**
         * Source is from current context ID.
         */
        CONTEXT,
    }

    /**
     * Represents a random number.
     */
    public static class RandomNumber {

        private final int lowerBound;

        private final int upperBound;

        /**
         * Creates a new instance.
         * @param lowerBound the lower bound (inclusive)
         * @param upperBound the upper bound (inclusive)
         */
        public RandomNumber(int lowerBound, int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        /**
         * Returns the lower bound of this random number.
         * @return the lower bound (inclusive)
         */
        public int getLowerBound() {
            return lowerBound;
        }

        /**
         * Returns the upper bound of this random number.
         * @return the upper bound (inclusive)
         */
        public int getUpperBound() {
            return upperBound;
        }
    }
}
