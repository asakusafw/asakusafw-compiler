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
package com.asakusafw.dag.runtime.directio;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.OutputTransactionContext;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;

/**
 * A transaction manager for Direct I/O file outputs.
 * @since 0.4.0
 */
public class TransactionManager {

    static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    private final Configuration configuration;

    private final String transactionId;

    private final Map<String, String> transactionProperties;

    private final Map<String, OutputTransactionContext> running = new HashMap<>();

    /**
     * Creates a new instance.
     * @param configuration the current Hadoop configuration
     * @param id the current transaction ID
     * @param properties the optional transaction properties
     */
    public TransactionManager(Configuration configuration, String id, Map<String, String> properties) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(properties);
        this.configuration = configuration;
        this.transactionId = id;
        this.transactionProperties = properties.isEmpty()
                ? Collections.singletonMap("Transaction ID", id) //$NON-NLS-1$
                : Arguments.freeze(properties);
    }

    /**
     * Returns the transaction ID.
     * @return the transaction ID
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Acquires a new {@link OutputTransactionContext} for the output.
     * @param id the target output ID
     * @return the acquired context
     */
    public synchronized OutputTransactionContext acquire(String id) {
        Arguments.requireNonNull(id);
        OutputTransactionContext acquired = running.get(id);
        if (acquired != null) {
            throw new IllegalStateException();
        }
        OutputTransactionContext created = new OutputTransactionContext(transactionId, id, new Counter());
        running.put(id, created);
        return created;
    }

    /**
     * Releases the {@link OutputTransactionContext}.
     * @param context the acquired context
     * @see #acquire(String)
     */
    public synchronized void release(OutputTransactionContext context) {
        Arguments.requireNonNull(context);
        String id = context.getOutputId();
        OutputTransactionContext acquired = running.get(id);
        if (acquired == null) {
            LOG.warn(MessageFormat.format(
                    "invalid transaction output ID: {0}",
                    id));
        }
        running.remove(id);
    }

    /**
     * Begins a transaction.
     * @throws IOException if I/O error was occurred while finalizing the current transaction
     */
    public void begin() throws IOException {
        LOG.debug("starting transaction of Direct I/O file output: {}", transactionId);
        setCommitted(true);
        setTransactionInfo(true);
    }

    /**
     * Ends the current transaction.
     * @throws IOException if I/O error was occurred while finalizing the current transaction
     */
    public void end() throws IOException {
        if (running.isEmpty()) {
            LOG.debug("finishing transaction of Direct I/O file output: {}", transactionId);
            if (isCommitted()) {
                setCommitted(false);
                setTransactionInfo(false);
            }
        }
    }

    private void setTransactionInfo(boolean value) throws IOException {
        Path transactionInfo = getTransactionInfoPath();
        FileSystem fs = transactionInfo.getFileSystem(configuration);
        if (value) {
            try (OutputStream output = new SafeOutputStream(fs.create(transactionInfo, false));
                    PrintWriter writer = new PrintWriter(
                            new OutputStreamWriter(output, HadoopDataSourceUtil.COMMENT_CHARSET))) {
                for (Map.Entry<String, String> entry : transactionProperties.entrySet()) {
                    if (entry.getValue() != null) {
                        writer.printf("%s: %s%n", //$NON-NLS-1$
                                entry.getKey(),
                                entry.getValue());
                    }
                }
            }
        } else {
            fs.delete(transactionInfo, false);
        }
    }

    private void setCommitted(boolean value) throws IOException {
        Path commitMark = getCommitMarkPath();
        FileSystem fs = commitMark.getFileSystem(configuration);
        if (value) {
            fs.create(commitMark, false).close();
        } else {
            fs.delete(commitMark, false);
        }
    }

    boolean isCommitted() throws IOException {
        Path commitMark = getCommitMarkPath();
        FileSystem fs = commitMark.getFileSystem(configuration);
        return fs.exists(commitMark);
    }

    private Path getTransactionInfoPath() throws IOException {
        return HadoopDataSourceUtil.getTransactionInfoPath(configuration, transactionId);
    }

    private Path getCommitMarkPath() throws IOException {
        return HadoopDataSourceUtil.getCommitMarkPath(configuration, transactionId);
    }

    private static class SafeOutputStream extends OutputStream {

        private final OutputStream delegate;

        private final AtomicBoolean closed = new AtomicBoolean();

        SafeOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                delegate.close();
            }
        }
    }
}
