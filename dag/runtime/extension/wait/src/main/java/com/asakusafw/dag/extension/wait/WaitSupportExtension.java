/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.dag.extension.wait;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.ProcessorContext.Editor;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Inserting wait time around DAG executions.
 * @since 0.4.0
 */
public class WaitSupportExtension implements ProcessorContextExtension {

    static final Logger LOG = LoggerFactory.getLogger(WaitSupportExtension.class);

    private static final String KEY_PREFIX = "com.asakusafw.dag.extension.wait."; //$NON-NLS-1$

    /**
     * The engine property key of waiting time before executes DAG (ms).
     */
    public static final String KEY_WAIT_BEFORE = KEY_PREFIX + "before"; //$NON-NLS-1$

    /**
     * The engine property key of waiting time after executes DAG (ms).
     */
    public static final String KEY_WAIT_AFTER = KEY_PREFIX + "after"; //$NON-NLS-1$

    @Override
    public InterruptibleIo install(ProcessorContext context, Editor editor) throws IOException, InterruptedException {
        LOG.debug("enabling wait support");
        beforeExecute(context);
        return () -> afterExecute(context);
    }

    private static void beforeExecute(ProcessorContext context) throws InterruptedException {
        wait(context, KEY_WAIT_BEFORE);
    }

    private static void afterExecute(ProcessorContext context) throws InterruptedException {
        wait(context, KEY_WAIT_AFTER);
    }

    private static void wait(ProcessorContext context, String key) throws InterruptedException {
        Optional<Long> time = context.getProperty(key)
            .flatMap(value -> {
                try {
                    long amount = Long.parseLong(value);
                    if (amount > 0L) {
                        return Optionals.of(amount);
                    }
                } catch (NumberFormatException e) {
                    LOG.warn(MessageFormat.format(
                            "must be a valid integer: {0}={1}",
                            key, value), e);
                }
                return Optionals.empty();
            });
        if (time.isPresent()) {
            LOG.info(MessageFormat.format("waiting {0}ms...",
                    time.get()));
            Thread.sleep(time.get());
        }
    }
}
