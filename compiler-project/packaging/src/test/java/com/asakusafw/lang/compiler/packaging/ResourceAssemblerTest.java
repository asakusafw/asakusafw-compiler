/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Test for {@link ResourceAssembler}.
 */
public class ResourceAssemblerTest extends ResourceTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addItem(item("a.txt", "A"));

        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(1));
        assertThat(items, hasEntry("a.txt", "A"));
    }

    /**
     * empty.
     * @throws Exception if failed
     */
    @Test
    public void no_items() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(0));
    }

    /**
     * multiple items.
     * @throws Exception if failed
     */
    @Test
    public void many_items() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addItem(item("a.txt", "A"));
        assembler.addItem(item("b.txt", "B"));
        assembler.addItem(item("c.txt", "C"));

        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add repository.
     * @throws Exception if failed
     */
    @Test
    public void repository() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"),
                item("b.txt", "B"),
                item("c.txt", "C"),
        }));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add multiple repositories.
     * @throws Exception if failed
     */
    @Test
    public void many_repository() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a0.txt", "A0"),
                item("b0.txt", "B0"),
        }));
        assembler.addRepository(repo(new ResourceItem[] {
                item("a1.txt", "A1"),
                item("b1.txt", "B1"),
        }));
        assembler.addRepository(repo(new ResourceItem[] {
                item("a2.txt", "A2"),
                item("b2.txt", "B2"),
        }));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(6));
        assertThat(items, hasEntry("a0.txt", "A0"));
        assertThat(items, hasEntry("a1.txt", "A1"));
        assertThat(items, hasEntry("a2.txt", "A2"));
        assertThat(items, hasEntry("b0.txt", "B0"));
        assertThat(items, hasEntry("b1.txt", "B1"));
        assertThat(items, hasEntry("b2.txt", "B2"));
    }

    /**
     * w/ duplicated resources.
     * @throws Exception if failed
     */
    @Test
    public void duplicate() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"),
                item("b.txt", "B"),
        }));
        assembler.addRepository(repo(new ResourceItem[] {
                item("b.txt", "B"),
                item("c.txt", "C"),
        }));
        assembler.addRepository(repo(new ResourceItem[] {
                item("c.txt", "C"),
                item("a.txt", "A"),
        }));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add repository w/ predicate.
     * @throws Exception if failed
     */
    @Test
    public void predicate() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"),
                item("b.bin", "B"), // filtered
                item("c.txt", "C"),
        }), pattern(".*\\.txt"));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(2));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add repository w/ multiple predicates.
     * @throws Exception if failed
     */
    @Test
    public void predicate_diff_repo() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"),
                item("b.bin", "B"),
        }), pattern(".*\\.txt"));
        assembler.addRepository(repo(new ResourceItem[] {
                item("c.txt", "C"),
                item("d.bin", "D"),
        }), pattern(".*\\.txt"));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(2));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add repository w/ multiple predicates.
     * @throws Exception if failed
     */
    @Test
    public void predicate_same_repo() throws Exception {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"),
                item("b.txt", "B"),
                item("c.txt", "C"), // filtered
                item("d.txt", "D"), // filtered
        }), pattern("[ab]\\.txt"));
        assembler.addRepository(repo(new ResourceItem[] {
                item("a.txt", "A"), // filtered
                item("b.txt", "B"),
                item("c.txt", "C"),
                item("d.txt", "D"), // filtered
        }), pattern("[bc]\\.txt"));
        Map<String, String> items = dump(assembler.build());
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    private ResourceItemRepository repo(ResourceItem... items) {
        return new ResourceItemRepository(Arrays.asList(items));
    }

    private Predicate<Location> pattern(String pattern) {
        Pattern p = Pattern.compile(pattern);
        return argument -> p.matcher(argument.toPath()).matches();
    }
}
