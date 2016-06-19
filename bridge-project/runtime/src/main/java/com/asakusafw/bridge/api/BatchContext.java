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
package com.asakusafw.bridge.api;

import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceCacheStorage;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.stage.StageConstants;

/**
 * Provides contextual information of the running batch.
 * <p>
 * Clients can use this class <em>only in operator methods</em>.
 * </p>
 *
 * <h3> requirements </h3>
 * <p>
 * This API requires that {@link StageInfo} object has been registered to {@link ResourceBroker}.
 * </p>
 * @see com.asakusafw.runtime.core.BatchContext
 * @since 0.1.0
 * @version 0.3.1
 */
public final class BatchContext {

    private static final ResourceCacheStorage<StageInfo> CACHE = new ResourceCacheStorage<>();

    private BatchContext() {
        return;
    }

    /**
     * Returns a batch argument.
     * @param name the argument name
     * @return the corresponded value, or {@code null} if the target argument is not defined
     * @throws IllegalArgumentException if the parameter is {@code null}
     * @throws IllegalStateException if the current session is wrong
     */
    public static String get(String name) {
        Objects.requireNonNull(name);
        StageInfo info = getStageInfo();
        String reserved = getReserved(name, info);
        if (reserved != null) {
            return reserved;
        }
        return info.getBatchArguments().get(name);
    }

    private static StageInfo getStageInfo() {
        StageInfo cached = CACHE.find();
        if (cached != null) {
            return cached;
        }
        StageInfo info = ResourceBroker.find(StageInfo.class);
        if (info == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "required resource has not been prepared yet: {0}",
                    StageInfo.class.getName()));
        }
        return CACHE.put(info);
    }

    private static String getReserved(String name, StageInfo info) {
        switch (name) {
        case StageConstants.VAR_USER:
            return info.getUserName();
        case StageConstants.VAR_BATCH_ID:
            return info.getBatchId();
        case StageConstants.VAR_FLOW_ID:
            return info.getFlowId();
        case StageConstants.VAR_EXECUTION_ID:
            return info.getExecutionId();
        default:
            return null;
        }
    }
}
