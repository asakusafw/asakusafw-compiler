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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.Group.Direction;
import com.asakusafw.lang.compiler.model.graph.Group.Ordering;

/**
 * Utilities for {@link Group}.
 */
public final class Groups {

    private static final Pattern PATTERN_ORDER = Pattern.compile(
            "(\\w+)"                                // 1 - asc //$NON-NLS-1$
            + "|" + "(\\+\\s*(\\w+))"               // 3 - asc //$NON-NLS-1$ //$NON-NLS-2$
            + "|" + "(-\\s*(\\w+))"                 // 5 - desc //$NON-NLS-1$ //$NON-NLS-2$
            + "|" + "((\\w+)\\s+[Aa][Ss][Cc])"      // 7 - asc //$NON-NLS-1$ //$NON-NLS-2$
            + "|" + "((\\w+)\\s+[Dd][Ee][Ss][Cc])"  // 9 - desc //$NON-NLS-1$ //$NON-NLS-2$
            );

    private static final Map<Integer, Direction> ORDER_GROUP_DIRECTIONS;
    static {
        Map<Integer, Direction> map = new LinkedHashMap<>();
        map.put(1, Direction.ASCENDANT);
        map.put(3, Direction.ASCENDANT);
        map.put(5, Direction.DESCENDANT);
        map.put(7, Direction.ASCENDANT);
        map.put(9, Direction.DESCENDANT);
        ORDER_GROUP_DIRECTIONS = map;
    }

    private Groups() {
        return;
    }

    /**
     * Parses string representation of a {@link Group}.
     * @param grouping the grouping property expressions
     * @return the related {@link Group} object
     * @throws IllegalArgumentException if some expressions are unrecognized
     */
    public static Group parse(List<String> grouping) {
        return parse(grouping, Collections.<String>emptyList());
    }

    /**
     * Parses string representation of a {@link Group}.
     * @param grouping the grouping property expressions
     * @param ordering the ordering property expressions
     * @return the related {@link Group} object
     * @throws IllegalArgumentException if some expressions are unrecognized
     */
    public static Group parse(List<String> grouping, List<String> ordering) {
        List<PropertyName> groupingResults = new ArrayList<>();
        for (String s : grouping) {
            groupingResults.add(PropertyName.of(s));
        }
        List<Ordering> orderingResults = new ArrayList<>();
        for (String s : ordering) {
            Ordering o = parseOrder(s);
            orderingResults.add(o);
        }
        return new Group(groupingResults, orderingResults);
    }

    /**
     * Parses string representation of an {@link Ordering}.
<pre><code>
Expression:
    Name
    '+' Name
    '-' Name
    Name 'ASC'
    Name 'DESC'
</code></pre>
     * @param expression the ordering property expression
     * @return the related {@link Ordering} object
     * @throws IllegalArgumentException if the expression is unrecognized
     */
    public static Ordering parseOrder(String expression) {
        Matcher matcher = PATTERN_ORDER.matcher(expression);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "invalid group ordering expression: {0}",
                    expression));
        }
        String name = null;
        Direction direction = null;
        for (Map.Entry<Integer, Direction> entry : ORDER_GROUP_DIRECTIONS.entrySet()) {
            int index = entry.getKey();
            String s = matcher.group(index);
            if (s != null) {
                name = s;
                direction = entry.getValue();
                break;
            }
        }
        assert name != null;
        assert direction != null;
        return new Ordering(PropertyName.of(name), direction);
    }
}
