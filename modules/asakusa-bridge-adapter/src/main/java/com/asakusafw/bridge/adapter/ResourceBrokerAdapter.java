package com.asakusafw.bridge.adapter;

import java.io.IOException;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceBroker.Initializer;
import com.asakusafw.bridge.broker.ResourceBroker.Scope;
import com.asakusafw.bridge.broker.ResourceSession;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.core.RuntimeResource;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * An adapter implementation between legacy Asakusa runtime resources and resource brokers.
 */
public class ResourceBrokerAdapter implements RuntimeResource {

    private ResourceSession current;

    @Override
    public void setup(final ResourceConfiguration configuration) throws IOException, InterruptedException {
        current = ResourceBroker.attach(Scope.THEAD, new Initializer() {
            @Override
            public void accept(ResourceSession session) throws IOException {
                StageInfo info = new StageInfo(
                        configuration.get(StageConstants.PROP_USER, System.getProperty("user.name")), //$NON-NLS-1$
                        configuration.get(StageConstants.PROP_BATCH_ID, null),
                        configuration.get(StageConstants.PROP_FLOW_ID, null),
                        null, // no stage ID info
                        configuration.get(StageConstants.PROP_EXECUTION_ID, null),
                        configuration.get(StageConstants.PROP_ASAKUSA_BATCH_ARGS, null));

                session.put(ResourceConfiguration.class, configuration);
                session.put(StageInfo.class, info);
            }
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
