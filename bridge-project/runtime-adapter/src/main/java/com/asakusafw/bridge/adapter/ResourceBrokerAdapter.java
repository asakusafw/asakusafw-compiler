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

import java.io.IOException;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceBroker.Scope;
import com.asakusafw.bridge.broker.ResourceSession;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.core.legacy.RuntimeResource;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * An adapter implementation between legacy Asakusa runtime resources and resource brokers.
 */
public class ResourceBrokerAdapter implements RuntimeResource {

    private ResourceSession current;

    @Override
    public void setup(ResourceConfiguration configuration) throws IOException, InterruptedException {
        current = ResourceBroker.attach(Scope.THREAD, session -> {
            StageInfo info = new StageInfo(
                    configuration.get(StageConstants.PROP_USER, System.getProperty("user.name")), //$NON-NLS-1$
                    configuration.get(StageConstants.PROP_BATCH_ID, null),
                    configuration.get(StageConstants.PROP_FLOW_ID, null),
                    null, // no stage ID info
                    configuration.get(StageConstants.PROP_EXECUTION_ID, null),
                    configuration.get(StageConstants.PROP_ASAKUSA_BATCH_ARGS, null));

            session.put(ResourceConfiguration.class, configuration);
            session.put(StageInfo.class, info);
        });
    }

    @Override
    public void cleanup(ResourceConfiguration configuration) throws IOException, InterruptedException {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
