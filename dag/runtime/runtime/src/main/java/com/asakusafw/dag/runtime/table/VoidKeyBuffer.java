/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.table;

import com.asakusafw.dag.runtime.adapter.KeyBuffer;

/**
 * A void implementation of {@link KeyBuffer}.
 * @since 0.4.0
 */
final class VoidKeyBuffer implements KeyBuffer, KeyBuffer.View {

    /**
     * The singleton instance.
     */
    public static final VoidKeyBuffer INSTANCE = new VoidKeyBuffer();

    private VoidKeyBuffer() {
        return;
    }

    @Override
    public View getView() {
        return this;
    }

    @Override
    public View getFrozen() {
        return this;
    }

    @Override
    public KeyBuffer clear() {
        return this;
    }

    @Override
    public KeyBuffer append(Object value) {
        return this;
    }
}
