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
package com.asakusafw.dag.extension.windows;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.ProcessorContext.Editor;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.runtime.windows.WindowsConfigurator;

/**
 * Activating Windows workarounds.
 * @since 0.5.0
 */
public class WindowsSupportExtension implements ProcessorContextExtension {

    static final Logger LOG = LoggerFactory.getLogger(WindowsSupportExtension.class);

    @Override
    public InterruptibleIo install(ProcessorContext context, Editor editor) throws IOException, InterruptedException {
        LOG.debug("enabling Windows workarond");
        WindowsConfigurator.install();
        return null;
    }
}
