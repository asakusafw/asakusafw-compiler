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
package com.asakusafw.bridge.adapter;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.flow.RuntimeResourceManager;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * Test for {@link ResourceBrokerAdapter}.
 */
public class ResourceBrokerAdapterTest {

    /**
     * setup/cleanup the resource brokers.
     */
    public final ResourceBrokerContext brokerContext = new ResourceBrokerContext();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(StageConstants.PROP_USER, "a");
        conf.set(StageConstants.PROP_BATCH_ID, "b");
        conf.set(StageConstants.PROP_FLOW_ID, "c");
        conf.set(StageConstants.PROP_EXECUTION_ID, "d");
        conf.set(StageConstants.PROP_ASAKUSA_BATCH_ARGS, "e=f");
        RuntimeResourceManager manager = new RuntimeResourceManager(conf);
        try {
            manager.setup();
            StageInfo info = ResourceBroker.get(StageInfo.class);
            assertThat(info.getUserName(), is("a"));
            assertThat(info.getBatchId(), is("b"));
            assertThat(info.getFlowId(), is("c"));
            assertThat(info.getExecutionId(), is("d"));
            assertThat(info.getStageId(), is(nullValue()));
            assertThat(info.getBatchArguments(), hasEntry("e", "f"));
        } finally {
            manager.cleanup();
        }
    }
}
