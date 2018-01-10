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
package com.asakusafw.lang.compiler.core.dummy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;

/**
 * Mock implementation of {@link JobflowProcessor}.
 */
public class SimpleBatchProcessor implements BatchProcessor {

    private static final Location MARKER = Location.of(SimpleBatchProcessor.class.getName(), '.');

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(BatchCompiler.Context context) {
        return containsFile(context);
    }

    private static boolean containsFile(BatchCompiler.Context context) {
        File file = context.getOutput().toFile(MARKER);
        return file.isFile();
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        try (OutputStream output = context.addResourceFile(MARKER)) {
            output.write("testing".getBytes("UTF-8"));
        }
    }
}
