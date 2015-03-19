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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

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
     * order w/ 'DESC'.
     */
    @Test
    public void order_desc() {
        Ordering o = Groups.parseOrder("a DESC");
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
}
