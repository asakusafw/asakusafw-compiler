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
package com.asakusafw.lang.compiler.model.description;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
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

    /**
     * test equalities.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        AnnotationDescription d0 = new AnnotationDescription(
                new ClassDescription("fake"),
                Collections.singletonMap("value", valueOf(100)));
        AnnotationDescription d1 = new AnnotationDescription(
                new ClassDescription("fake"),
                Collections.singletonMap("value", valueOf(100)));
        AnnotationDescription d2 = new AnnotationDescription(
                new ClassDescription("other"),
                Collections.singletonMap("value", valueOf(100)));
        AnnotationDescription d3 = new AnnotationDescription(
                new ClassDescription("fake"),
                Collections.singletonMap("value", valueOf(200)));

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d1.toString(), d1.hashCode(), is(d0.hashCode()));
        assertThat(d1.toString(), d2, is(not(d0)));
        assertThat(d1.toString(), d3, is(not(d0)));
    }
}
