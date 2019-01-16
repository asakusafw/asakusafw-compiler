/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * Test for {@link ResourceItemRepository}.
 */
public class ResourceItemRepositoryTest extends ResourceTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        List<ResourceItem> items = new ArrayList<>();
        items.add(item("a.txt", "A"));

        ResourceItemRepository repo = new ResourceItemRepository(items);
        Map<String, String> results = dump(repo);
        assertThat(results.keySet(), hasSize(1));
        assertThat(results, hasEntry("a.txt", "A"));
    }

    /**
     * multiple items.
     */
    @Test
    public void many() {
        List<ResourceItem> items = new ArrayList<>();
        items.add(item("a.txt", "A"));
        items.add(item("b.txt", "B"));
        items.add(item("c.txt", "C"));

        ResourceItemRepository repo = new ResourceItemRepository(items);
        Map<String, String> results = dump(repo);
        assertThat(results.keySet(), hasSize(3));
        assertThat(results, hasEntry("a.txt", "A"));
        assertThat(results, hasEntry("b.txt", "B"));
        assertThat(results, hasEntry("c.txt", "C"));
    }

    /**
     * empty.
     */
    @Test
    public void no_items() {
        List<ResourceItem> items = new ArrayList<>();
        ResourceItemRepository repo = new ResourceItemRepository(items);
        Map<String, String> results = dump(repo);
        assertThat(results.keySet(), hasSize(0));
    }

    /**
     * comparing.
     */
    @Test
    public void compare() {
        List<ResourceItem> items = new ArrayList<>();
        items.add(item("a.txt", "A"));
        items.add(item("b.txt", "B"));
        items.add(item("c.txt", "C"));

        ResourceItemRepository repo = new ResourceItemRepository(items);
        assertThat(repo.toString(), repo, is(new ResourceItemRepository(items)));
        assertThat(repo.hashCode(), is(new ResourceItemRepository(items).hashCode()));
        assertThat(repo, is(not(new ResourceItemRepository(items.subList(0, 2)))));
    }
}
