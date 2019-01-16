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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * An abstract super interface of adapters used in each vertex operation.
 * @since 0.4.0
 */
public interface VertexElementAdapter extends InterruptibleIo, InterruptibleIo.IoInitializable {

    @Override
    default void initialize() throws IOException, InterruptedException {
        return;
    }

    @Override
    default void close() throws IOException, InterruptedException {
        return;
    }
}
