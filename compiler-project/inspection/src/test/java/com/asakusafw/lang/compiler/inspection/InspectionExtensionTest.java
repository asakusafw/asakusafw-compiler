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
package com.asakusafw.lang.compiler.inspection;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * Test for {@link InspectionExtension}.
 */
public class InspectionExtensionTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        AtomicReference<Object> ref = new AtomicReference<>();
        ExtensionContainer container = container(new InspectionExtension() {
            @Override
            public boolean isSupported(Object element) {
                return true;
            }
            @Override
            public void inspect(Location location, Object element) {
                assertThat(location, is(Location.of("testing")));
                assertThat(ref.compareAndSet(null, element), is(true));
            }
            @Override
            public void inspect(Path path, Object element) {
                throw new AssertionError();
            }
        });
        Object object = new Object();
        InspectionExtension.inspect(container, Location.of("testing"), object);
        assertThat(ref.get(), is(sameInstance(object)));
    }

    /**
     * unsupported type.
     */
    @Test
    public void unsupported() {
        ExtensionContainer container = container(new InspectionExtension() {
            @Override
            public boolean isSupported(Object element) {
                return false;
            }
            @Override
            public void inspect(Location location, Object element) {
                throw new AssertionError();
            }
            @Override
            public void inspect(Path path, Object element) {
                throw new AssertionError();
            }
        });
        InspectionExtension.inspect(container, Location.of("testing"), new Object());
        // no exceptions
    }

    /**
     * extension is not registered.
     */
    @Test
    public void not_registered() {
        ExtensionContainer container = new BasicExtensionContainer();
        InspectionExtension.inspect(container, Location.of("testing"), new Object());
        // no exceptions
    }

    private ExtensionContainer container(InspectionExtension extension) {
        BasicExtensionContainer container = new BasicExtensionContainer();
        container.registerExtension(InspectionExtension.class, extension);
        return container;
    }
}
