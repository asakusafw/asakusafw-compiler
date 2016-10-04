/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

import org.junit.Test;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link ApplicationGenerator}.
 */
public class ApplicationGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Location location = Location.of("com/example/app.bin");
        GraphInfo info = new GraphInfo();
        save(location, info);
        ClassDescription generated = add(c -> new ApplicationGenerator().generate(location, c));
        loading(generated, c -> {
            @SuppressWarnings("unchecked")
            Supplier<GraphInfo> s = (Supplier<GraphInfo>) c.newInstance();
            GraphInfo restored = s.get();
            assertThat(restored.getVertices(), hasSize(0));
            assertThat(restored.getEdges(), hasSize(0));
        });
    }

    private void save(Location location, GraphInfo info) {
        try (OutputStream output = classpath().addResource(location)) {
            GraphInfo.save(output, info);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
