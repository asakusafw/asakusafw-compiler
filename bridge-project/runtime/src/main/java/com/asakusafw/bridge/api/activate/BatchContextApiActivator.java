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
package com.asakusafw.bridge.api.activate;

import com.asakusafw.bridge.api.BatchContext;
import com.asakusafw.runtime.core.api.ApiStub.Reference;
import com.asakusafw.runtime.core.api.BatchContextApi;

/**
 * Activates {@link BatchContext}.
 * @since 0.4.0
 */
public class BatchContextApiActivator implements ApiActivator {

    private static final BatchContextApi API = new BatchContextApi() {
        @Override
        public String get(String name) {
            return BatchContext.get(name);
        }
    };

    @Override
    public Reference<BatchContextApi> activate() {
        return com.asakusafw.runtime.core.BatchContext.getStub().activate(API);
    }
}
