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
package com.asakusafw.bridge.api;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceBrokerContext;
import com.asakusafw.bridge.stage.StageInfo;

/**
 * Test for {@link BatchContext}.
 */
public class BatchContextTest {

    /**
     * setup/cleanup the test case.
     */
    @Rule
    public final ResourceBrokerContext brokerContext = new ResourceBrokerContext(true);

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ResourceBroker.put(StageInfo.class, info("a", "Hello, world!"));
        assertThat(BatchContext.get("a"), is("Hello, world!"));
        assertThat(BatchContext.get("b"), is(nullValue()));
    }

    /**
     * w/o required resource.
     */
    @Test(expected = IllegalStateException.class)
    public void not_prepared() {
        BatchContext.get("a");
    }

    private static StageInfo info(String... args) {
        assertThat(args.length % 2, is(0));
        Map<String, String> batchArguments = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            batchArguments.put(args[i + 0], args[i + 1]);
        }
        return new StageInfo("u", "b", "f", "s", "e", batchArguments);
    }
}
