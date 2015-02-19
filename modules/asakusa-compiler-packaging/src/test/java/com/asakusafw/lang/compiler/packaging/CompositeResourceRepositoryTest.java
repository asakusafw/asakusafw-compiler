package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * Test for {@link CompositeResourceRepository}.
 */
public class CompositeResourceRepositoryTest extends ResourceTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ResourceRepository r0 = repository(item("a.txt", "A"));

        ResourceRepository repo = new CompositeResourceRepository(Arrays.asList(r0));
        Map<String, String> results = dump(repo);
        assertThat(results.keySet(), hasSize(1));
        assertThat(results, hasEntry("a.txt", "A"));
    }

    /**
     * multiple items.
     */
    @Test
    public void many() {
        ResourceRepository r0 = repository(item("a.txt", "A"));
        ResourceRepository r1 = repository(item("b.txt", "B"), item("c.txt", "C"));
        ResourceRepository r2 = repository();

        ResourceRepository repo = new CompositeResourceRepository(Arrays.asList(r0, r1, r2));
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
        ResourceRepository repo = new CompositeResourceRepository(Collections.<ResourceRepository>emptyList());
        Map<String, String> results = dump(repo);
        assertThat(results.keySet(), hasSize(0));
    }

    /**
     * comparing.
     */
    @Test
    public void compare() {
        List<ResourceRepository> elements = new ArrayList<>();
        elements.add(repository(item("a.txt", "A")));
        elements.add(repository(item("b.txt", "B"), item("c.txt", "C")));
        elements.add(repository());

        CompositeResourceRepository repo = new CompositeResourceRepository(elements);
        assertThat(repo.toString(), repo, is(new CompositeResourceRepository(elements)));
        assertThat(repo.hashCode(), is(new CompositeResourceRepository(elements).hashCode()));
        assertThat(repo, is(not(new CompositeResourceRepository(elements.subList(0, 2)))));
    }
}
