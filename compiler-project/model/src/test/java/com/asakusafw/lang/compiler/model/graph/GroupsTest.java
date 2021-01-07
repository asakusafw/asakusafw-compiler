/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.Group.Direction;
import com.asakusafw.lang.compiler.model.graph.Group.Ordering;

/**
 * Test for {@link Groups}.
 */
public class GroupsTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Group g = Groups.parse(Arrays.asList("a"));
        assertThat(g.getGrouping(), contains(PropertyName.of("a")));
        assertThat(g.getOrdering(), is(empty()));
    }

    /**
     * w/ orderings.
     */
    @Test
    public void ordering() {
        Group g = Groups.parse(Arrays.asList("a"), Arrays.asList("b"));
        assertThat(g.getGrouping(), contains(PropertyName.of("a")));
        assertThat(g.getOrdering(), contains(new Ordering(PropertyName.of("b"), Direction.ASCENDANT)));
    }

    /**
     * order w/o symbols.
     */
    @Test
    public void order_implicit() {
        Ordering o = Groups.parseOrder("a");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.ASCENDANT));
    }

    /**
     * order w/ plus.
     */
    @Test
    public void order_plus() {
        Ordering o = Groups.parseOrder("+a");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.ASCENDANT));
    }

    /**
     * order w/ minus.
     */
    @Test
    public void order_minus() {
        Ordering o = Groups.parseOrder("-a");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.DESCENDANT));
    }

    /**
     * order w/ 'ASC'.
     */
    @Test
    public void order_asc() {
        Ordering o = Groups.parseOrder("a ASC");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.ASCENDANT));
    }

    /**
     * order w/ 'asc'.
     */
    @Test
    public void order_asc_lower() {
        Ordering o = Groups.parseOrder("a asc");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.ASCENDANT));
    }

    /**
     * order w/ 'DESC'.
     */
    @Test
    public void order_desc() {
        Ordering o = Groups.parseOrder("a DESC");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.DESCENDANT));
    }

    /**
     * order w/ 'desc'.
     */
    @Test
    public void order_desc_lower() {
        Ordering o = Groups.parseOrder("a desc");
        assertThat(o.getPropertyName(), is(PropertyName.of("a")));
        assertThat(o.getDirection(), is(Direction.DESCENDANT));
    }

    /**
     * order w/ unrecognized symbols.
     */
    @Test(expected = IllegalArgumentException.class)
    public void order_unrecognized() {
        Groups.parseOrder("?a");
    }

    /**
     * test for equality.
     */
    @Test
    public void equality() {
        Group g0 = group("=g0", "=g1", "+o0", "-o1");
        Group g1 = group("=g0", "=g1", "+o0", "-o1");
        Group g2 = group("=g0", "=g2", "+o0", "-o1");
        Group g3 = group("=g0", "=g1", "-o0", "-o1");
        Group g4 = group("=g0", "=g1", "+o0", "-o2");

        assertThat(g1.toString(), g1, is(g0));
        assertThat(g1.toString(), g1.hashCode(), is(g0.hashCode()));
        assertThat(g2.toString(), g2, is(not(g0)));
        assertThat(g3.toString(), g3, is(not(g0)));
        assertThat(g4.toString(), g4, is(not(g0)));
    }

    /**
     * test for subgroups.
     */
    @Test
    public void subgroup() {
        assertThat(group("=k"), is(subgroupOf("=k")));
        assertThat(group("=k"), is(not(subgroupOf("=x"))));
        assertThat(group("=k"), is(not(subgroupOf("=k", "=l"))));

        assertThat(group("=k", "+a"), is(subgroupOf("=k")));
        assertThat(group("=k", "+a"), is(subgroupOf("=k", "+a")));
        assertThat(group("=k", "+a"), is(not(subgroupOf("=k", "+a", "+b"))));
        assertThat(group("=k", "+a"), is(not(subgroupOf("=k", "-a"))));
    }

    private static Group group(String... expressions) {
        List<String> grouping = new ArrayList<>();
        List<String> ordering = new ArrayList<>();
        for (String s : expressions) {
            if (s.startsWith("=")) {
                grouping.add(s.substring(1));
            } else {
                ordering.add(s);
            }
        }
        return Groups.parse(grouping, ordering);
    }

    private static Matcher<Group> subgroupOf(String... expressions) {
        Group b = group(expressions);
        return new BaseMatcher<Group>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("subgroup of ").appendValue(b);
            }
            @Override
            public boolean matches(Object item) {
                Group a = (Group) item;
                return Groups.isSubGroup(a, b);
            }
        };
    }
}
