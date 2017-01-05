/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * Test for {@link ObjectComparatorGenerator}.
 */
public class ObjectComparatorGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        test(group("=key", "+sort"), model(0, "1.0", "A"), model(1, "1.0", "B"), equalTo(0));
        test(group("=key", "+sort"), model(0, "1.0", "A"), model(1, "2.0", "B"), lessThan(0));
        test(group("=key", "-sort"), model(0, "1.0", "A"), model(1, "2.0", "B"), greaterThan(0));
    }

    /**
     * w/ multiple entries.
     */
    @Test
    public void multiple() {
        test(group("+key", "+sort", "-value"), model(0, "1.0", "A"), model(1, "1.0", "A"), lessThan(0));
        test(group("+key", "+sort", "-value"), model(0, "1.0", "A"), model(0, "2.0", "A"), lessThan(0));
        test(group("+key", "+sort", "-value"), model(0, "1.0", "A"), model(0, "1.0", "B"), greaterThan(0));
        test(group("+key", "+sort", "-value"), model(0, "1.0", "A"), model(0, "1.0", "A"), equalTo(0));
    }

    /**
     * cache - equivalent.
     */
    @Test
    public void cache() {
        ClassData a = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order("+sort"));
        ClassData b = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order("+sort"));
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache - trivial.
     */
    @Test
    public void cache_trivial() {
        ClassData a = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order());
        ClassData b = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order());
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache w/ different types.
     */
    @Test
    public void cache_diff_type() {
        ClassData a = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order("+key"));
        ClassData b = ObjectComparatorGenerator.generate(context(), typeOf(MockKeyValueModel.class), order("+key"));
        assertThat(b, is(not(cacheOf(a))));
    }

    /**
     * cache w/ different groupings.
     */
    @Test
    public void cache_diff_group() {
        ClassData a = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order("+sort"));
        ClassData b = ObjectComparatorGenerator.generate(context(), typeOf(MockDataModel.class), order("-sort"));
        assertThat(b, is(not(cacheOf(a))));
    }

    private MockDataModel model(int key, String sort, String value) {
        return new MockDataModel(key, sort == null ? null : new BigDecimal(sort), value);
    }

    private void test(Group group, MockDataModel a, MockDataModel b, Matcher<Integer> predicate) {
        ClassDescription type = classOf(MockDataModel.class);
        ClassDescription comparator = ObjectComparatorGenerator.get(context(), type, group.getOrdering());
        loading(cl -> {
            @SuppressWarnings("unchecked")
            Comparator<Object> cmp = (Comparator<Object>) comparator.resolve(cl).newInstance();
            assertThat(cmp.compare(a, b), predicate);
        });
    }

    private List<Group.Ordering> order(String... expressions) {
        Group group = group(expressions);
        assertThat(group.getGrouping(), hasSize(0));
        return group.getOrdering();
    }
}
