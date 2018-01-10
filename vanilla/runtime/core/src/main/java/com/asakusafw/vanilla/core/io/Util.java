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
package com.asakusafw.vanilla.core.io;

import com.asakusafw.vanilla.core.util.ExtensibleDataBuffer;

final class Util {

    static final int MIN_BUFFER_SIZE = 64 * 1024;

    private static final double MIN_BUFFER_FLUSH_FACTOR = 0.10;

    private static final double MAX_BUFFER_FLUSH_FACTOR = 0.99;

    static final double DEFAULT_BUFFER_FLUSH_FACTOR = 0.90;

    static int getBufferSize(int limit) {
        return Math.max(limit, MIN_BUFFER_SIZE);
    }

    static ExtensibleDataBuffer newDataBuffer(int limit) {
        return new ExtensibleDataBuffer(MIN_BUFFER_SIZE, getBufferSize(limit));
    }

    static int getBufferThreshold(int limit, double flushFactor) {
        double factor = Math.min(Math.max(flushFactor, MIN_BUFFER_FLUSH_FACTOR), MAX_BUFFER_FLUSH_FACTOR);
        int size = getBufferSize(limit);
        return (int) (size * factor);
    }

    private Util() {
        return;
    }
}
