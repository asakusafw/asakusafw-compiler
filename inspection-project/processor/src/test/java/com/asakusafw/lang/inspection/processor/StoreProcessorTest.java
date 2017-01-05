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
package com.asakusafw.lang.inspection.processor;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNodeRepository;
import com.asakusafw.lang.inspection.json.JsonInspectionNodeRepository;
import com.asakusafw.lang.inspection.processor.InspectionNodeProcessor.Context;

/**
 * Test for {@link StoreProcessor}.
 */
public class StoreProcessorTest {

    private final InspectionNodeRepository repository = new JsonInspectionNodeRepository();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InspectionNode node = new InspectionNode("simple", "TESTING");
        InspectionNode restored;
        byte[] bytes = process(node);
        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes)) {
            restored = repository.load(input);
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        assertThat(restored.getId(), is("simple"));
        assertThat(restored.getTitle(), is("TESTING"));
    }

    private byte[] process(InspectionNode node) {
        return process(context(), node);
    }

    private Context context(String... pairs) {
        assertThat(pairs.length % 2, is(0));
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i + 0], pairs[i + 1]);
        }
        return new Context(map);
    }

    private byte[] process(Context context, InspectionNode node) {
        StoreProcessor proc = new StoreProcessor(repository);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            proc.process(context, node, output);
            return output.toByteArray();
        } catch (IOException e) {
            throw new AssertionError();
        }
    }
}
