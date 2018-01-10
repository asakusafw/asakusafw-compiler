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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.InterruptibleIo.IoCallable;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Limits the max concurrency of {@link TaskProcessor}.
 * @since 0.4.1
 */
class ConcurrencyLimitter implements InterruptibleIo, IoCallable<TaskProcessor> {

    static final Logger LOG = LoggerFactory.getLogger(ConcurrencyLimitter.class);

    private final Semaphore semaphore;

    private final Queue<TaskProcessor> waiting = new ConcurrentLinkedQueue<>();

    private final Closer closer;

    ConcurrencyLimitter(
            IoCallable<? extends TaskProcessor> factory,
            int maxConcurrency) throws IOException, InterruptedException {
        Arguments.require(maxConcurrency > 0);
        this.semaphore = new Semaphore(maxConcurrency);
        try (Closer c = new Closer()) {
            for (int i = 0, n = maxConcurrency; i < n; i++) {
                waiting.add(c.add(factory.call()));
            }
            closer = c.move();
        }
    }

    @Override
    public TaskProcessor call() throws IOException, InterruptedException {
        return new Waiter(semaphore, waiting);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closer.close();
    }

    private static final class Waiter implements TaskProcessor {

        private final Semaphore semaphore;

        private final Queue<TaskProcessor> waiting;

        Waiter(Semaphore semaphore, Queue<TaskProcessor> waiting) {
            this.semaphore = semaphore;
            this.waiting = waiting;
        }

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            lock();
            try {
                TaskProcessor task = waiting.poll();
                Invariants.requireNonNull(task);
                try {
                    task.run(context);
                } finally {
                    waiting.offer(task);
                }
            } finally {
                unlock();
            }
        }

        private void lock() throws InterruptedException {
            if (LOG.isTraceEnabled() == false) {
                semaphore.acquire();
            } else {
                if (semaphore.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                    LOG.trace("no blocking by concurrency limitter: rest {} permits", //$NON-NLS-1$
                            semaphore.availablePermits());
                    return;
                }
                long waitBegin = System.currentTimeMillis();
                LOG.trace("start blocking by concurrency limitter"); //$NON-NLS-1$
                semaphore.acquire();
                LOG.trace("finish blocking by concurrency limitter: {}ms", //$NON-NLS-1$
                        String.format("%,d", System.currentTimeMillis() - waitBegin)); //$NON-NLS-1$
            }
        }

        private void unlock() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("releasing lock of concurrency limitter");
            }
            semaphore.release();
        }
    }
}
