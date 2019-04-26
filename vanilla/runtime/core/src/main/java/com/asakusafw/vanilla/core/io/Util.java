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
package com.asakusafw.vanilla.core.io;

import com.asakusafw.vanilla.core.util.ExtensibleDataBuffer;

final class Util {

    static final int MIN_BUFFER_SIZE = 64 * 1024;

    private static final int MIN_BUFFER_MARGIN_SIZE = 256;

    static final int DEFAULT_BUFFER_MARGIN_SIZE = 256 * 1024;

    static int getBufferSize(int limit) {
        return Math.max(limit, MIN_BUFFER_SIZE);
    }

    static ExtensibleDataBuffer newDataBuffer(int limit) {
        return new ExtensibleDataBuffer(MIN_BUFFER_SIZE, getBufferSize(limit));
    }

    static int getBufferThreshold(int limit, int marginSize) {
        int margin = Math.min(Math.max(marginSize, MIN_BUFFER_MARGIN_SIZE), (limit + 1) / 2);
        return limit - margin;
    }

    private Util() {
        return;
    }
}
