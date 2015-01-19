package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link AnnotationDescription}.
 */
public class AnnotationDescriptionTest {

    /**
     * Test information collector.
     */
    @Rule
    public final TestWatcher collector = new TestWatcher() {
        @Override
        protected void starting(org.junit.runner.Description d) {
            AnnotationDescriptionTest.this.description = d;
        }
    };

    Description description;

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Test annotation = description.getAnnotation(Test.class);
        AnnotationDescription desc = AnnotationDescription.of(annotation);
        assertThat(desc.getValueKind(), is(ValueKind.ANNOTATION));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) Test.class));

        Map<String, ValueDescription> elements = desc.getElements();
        assertThat(elements.keySet(), hasItem("expected"));
        assertThat(elements.keySet(), hasItem("timeout"));

        Test resolved = (Test) desc.resolve(getClass().getClassLoader());
        assertThat(resolved.annotationType(), is((Object) Test.class));
        assertThat(resolved.expected(), is((Object) annotation.expected()));
        assertThat(resolved.timeout(), is((Object) annotation.timeout()));

        // check basic methods
        assertThat(resolved.hashCode(), is(resolved.hashCode()));
        assertThat(resolved, equalTo(resolved));
        assertThat(resolved.toString(), is(resolved.toString()));
    }
}
