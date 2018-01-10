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
package com.asakusafw.dag.extension.trace;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.Reportable;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Reports digests of each port I/O.
 * @since 0.4.0
 */
public class PortDigester implements PortTracer, Reportable {

    static final Logger LOG = LoggerFactory.getLogger(PortDigester.class);

    private final ConcurrentMap<Tuple<String, String>, Digester> entries = new ConcurrentHashMap<>();

    @Override
    public boolean isSupported(String vertexId) {
        return LOG.isInfoEnabled();
    }

    @Override
    public Consumer<Object> getSink(String vertexId, String portId) {
        Arguments.requireNonNull(vertexId);
        Arguments.requireNonNull(portId);
        Tuple<String, String> key = new Tuple<>(vertexId, portId);
        return entries.computeIfAbsent(key, k -> new Digester());
    }

    @Override
    public void report() throws IOException, InterruptedException {
        SortedMap<Tuple<String, String>, Digester> copy = new TreeMap<>((a, b) -> {
            int left = a.left().compareTo(b.left());
            if (left != 0) {
                return left;
            }
            return a.right().compareTo(b.right());
        });
        copy.putAll(entries);
        LOG.info(String.format("Port I/O digest: %,d entries", copy.size()));
        copy.forEach((pair, digester) -> {
            String vId = pair.left();
            String pId = pair.right();
            long count = digester.count.get();
            int digest = digester.digest.get();
            LOG.info(String.format("  port=%s.%s, count=%,d, digest=%08x",
                    vId, pId,
                    count,
                    digest));
        });
    }

    private static final class Digester implements Consumer<Object> {

        final AtomicLong count = new AtomicLong();

        final AtomicInteger digest = new AtomicInteger();

        Digester() {
            return;
        }

        @Override
        public void accept(Object t) {
            count.incrementAndGet();
            digest.addAndGet(Objects.hashCode(t));
        }
    }
}
