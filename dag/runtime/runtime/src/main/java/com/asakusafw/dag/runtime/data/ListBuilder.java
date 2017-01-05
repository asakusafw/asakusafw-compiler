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
package com.asakusafw.dag.runtime.data;

import java.io.IOException;
import java.util.List;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Provides list.
 * @param <T> the data type
 * @since 0.4.1
 */
public interface ListBuilder<T> extends InterruptibleIo {

    /**
     * Builds a list from the given {@link ObjectCursor}.
     * This operation will change the previously returned list.
     * @param cursor the source cursor
     * @return the list
     * @throws IOException if I/O error was occurred while building a list
     * @throws InterruptedException if interrupted while building a list
     */
    List<T> build(ObjectCursor cursor) throws IOException, InterruptedException;
}
