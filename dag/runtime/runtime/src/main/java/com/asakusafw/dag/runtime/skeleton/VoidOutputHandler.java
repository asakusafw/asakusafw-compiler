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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;

/**
 * An {@link OutputHandler} which can accept nothing.
 * @since 0.4.0
 */
public final class VoidOutputHandler implements OutputHandler<ProcessorContext> {

    /**
     * The singleton instance.
     */
    public static final VoidOutputHandler INSTANCE = new VoidOutputHandler();

    private VoidOutputHandler() {
        return;
    }

    @Override
    public boolean contains(String id) {
        return false;
    }

    @Override
    public Session start(ProcessorContext context) throws IOException, InterruptedException {
        return () -> Lang.pass();
    }

    @Override
    public <T> Result<T> getSink(Class<T> type, String id) {
        throw new NoSuchElementException(id);
    }
}
