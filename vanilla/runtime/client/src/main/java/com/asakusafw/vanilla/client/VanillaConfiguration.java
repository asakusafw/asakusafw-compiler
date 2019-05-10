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
package com.asakusafw.vanilla.client;

import static com.asakusafw.vanilla.client.VanillaConstants.*;

import java.io.File;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vanilla.core.io.BufferedByteChannelDecorator;
import com.asakusafw.vanilla.core.io.ByteChannelDecorator;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * A configuration of Asakusa Vanilla runtime.
 * @since 0.4.0
 * @version 0.5.3
 */
public class VanillaConfiguration {

    /**
     * The configuration key of max worker threads ({@value}: {@value #DEFAULT_THREAD_COUNT}).
     */
    public static final String KEY_THREAD_COUNT = KEY_ENGINE_PREFIX + "thread.max"; //$NON-NLS-1$

    /**
     * The configuration key of default number of partitions ({@value} = {@link #KEY_THREAD_COUNT}).
     */
    public static final String KEY_PARTITION_COUNT = KEY_ENGINE_PREFIX + "partitions"; //$NON-NLS-1$

    /**
     * The configuration key of buffer pool size in bytes({@value}: {@value #DEFAULT_BUFFER_POOL_SIZE}).
     */
    public static final String KEY_BUFFER_POOL_SIZE = KEY_ENGINE_PREFIX + "pool.size"; //$NON-NLS-1$

    /**
     * The configuration key of buffer pool swap area on the local file system
     * ({@value}: {@link #DEFAULT_SWAP_DIRECTORY}).
     */
    public static final String KEY_SWAP_DIRECTORY = KEY_ENGINE_PREFIX + "pool.swap"; //$NON-NLS-1$

    /**
     * The configuration key of max entries in each swap sub-directory, or {@code 0} to disable it
     * ({@value}: {@value #DEFAULT_SWAP_DIVISION}).
     * @since 0.4.1
     */
    public static final String KEY_SWAP_DIVISION = KEY_ENGINE_PREFIX + "pool.division"; //$NON-NLS-1$

    /**
     * The configuration key of codec class name of buffer pool swap files.
     * This must be a sub-class of {@link ByteChannelDecorator}.
     * @since 0.5.3
     */
    public static final String KEY_SWAP_DECORATOR = KEY_ENGINE_PREFIX + "pool.compression"; //$NON-NLS-1$

    /**
     * The configuration key of output buffer size in bytes ({@value}: {@value #DEFAULT_OUTPUT_BUFFER_SIZE}).
     */
    public static final String KEY_OUTPUT_BUFFER_SIZE = KEY_ENGINE_PREFIX + "output.buffer.size"; //$NON-NLS-1$

    /**
     * The configuration key of output buffer margin size in bytes ({@value}: {@value #DEFAULT_OUTPUT_BUFFER_MARGIN}).
     * If {@link #KEY_OUTPUT_BUFFER_SIZE} - {@link #KEY_OUTPUT_BUFFER_MARGIN} was used,
     * the output buffer will be flushed.
     * @since 0.5.3
     */
    public static final String KEY_OUTPUT_BUFFER_MARGIN = KEY_ENGINE_PREFIX + "output.buffer.margin"; //$NON-NLS-1$

    /**
     * The configuration key of the estimated average output record size
     * ({@value}: {@value #DEFAULT_OUTPUT_RECORD_SIZE}).
     * This is for computing the record capacity in individual output buffers.
     */
    public static final String KEY_OUTPUT_RECORD_SIZE = KEY_ENGINE_PREFIX + "output.record.size"; //$NON-NLS-1$

    /**
     * The configuration key of the maximum number of merging scatter/gather input chunks at one time,
     * or {@code 0} as unlimited.
     * If the limit was exceeded, the engine will merge into single chunk before using them.
     * @since 0.5.3
     */
    public static final String KEY_MERGE_THRESHOLD = KEY_ENGINE_PREFIX + "merge.threshold"; //$NON-NLS-1$

    /**
     * The configuration key of the fraction of the number of merging scatter/gather input chunks,
     * it the {@link #KEY_MERGE_THRESHOLD} was exceeded.
     * @since 0.5.3
     */
    public static final String KEY_MERGE_FACTOR = KEY_ENGINE_PREFIX + "merge.factor"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_THREAD_COUNT}.
     */
    public static final int DEFAULT_THREAD_COUNT = 1;

    /**
     * The default value of {@link #KEY_BUFFER_POOL_SIZE}.
     */
    public static final long DEFAULT_BUFFER_POOL_SIZE = 256L * 1024L * 1024L;

    /**
     * The default value of {@link #KEY_SWAP_DIRECTORY} ({@code System.getProperty("java.io.tmpdir")}).
     */
    public static final File DEFAULT_SWAP_DIRECTORY = SystemProperty.getTemporaryDirectory();

    /**
     * The default value of {@link #KEY_SWAP_DIVISION} (disabled).
     * @since 0.4.1
     */
    public static final int DEFAULT_SWAP_DIVISION = 0;

    /**
     * The default value of {@link #KEY_SWAP_DECORATOR} (only buffered).
     * @since 0.5.3
     * @see BufferedByteChannelDecorator
     */
    public static final SupplierInfo DEFAULT_SWAP_DECORATOR =
            SupplierInfo.of(BufferedByteChannelDecorator.class.getName());

    /**
     * The default value of {@link #KEY_OUTPUT_BUFFER_SIZE}.
     */
    public static final int DEFAULT_OUTPUT_BUFFER_SIZE = 4 * 1024 * 1024;

    /**
     * The default value of {@link #KEY_OUTPUT_RECORD_SIZE}.
     */
    public static final int DEFAULT_OUTPUT_RECORD_SIZE = 64;

    /**
     * The default value of {@link #KEY_OUTPUT_BUFFER_MARGIN}.
     */
    public static final int DEFAULT_OUTPUT_BUFFER_MARGIN = 1 * 1024 * 1024;

    /**
     * The default value of {@link #KEY_MERGE_THRESHOLD}.
     * @since 0.5.3
     */
    public static final int DEFAULT_MERGE_THRESHOLD = 0;

    /**
     * The default value of {@link #KEY_MERGE_FACTOR}.
     * @since 0.5.3
     */
    public static final double DEFAULT_MERGE_FACTOR = 1.0;

    static final Logger LOG = LoggerFactory.getLogger(VanillaConfiguration.class);

    private OptionalInt numberOfThreads = OptionalInt.empty();

    private OptionalInt numberOfPartitions = OptionalInt.empty();

    private OptionalLong bufferPoolSize = OptionalLong.empty();

    private Optional<File> swapDirectory = Optional.empty();

    private OptionalInt swapDivision = OptionalInt.empty();

    private Optional<SupplierInfo> swapDecorator = Optional.empty();

    private OptionalInt outputBufferSize = OptionalInt.empty();

    private OptionalInt outputBufferMargin = OptionalInt.empty();

    private OptionalInt outputRecordSize = OptionalInt.empty();

    private OptionalInt mergeThreshold = OptionalInt.empty();

    private OptionalDouble mergeFactor = OptionalDouble.empty();

    /**
     * Returns the number of worker threads.
     * @return the number of worker threads
     * @see #KEY_THREAD_COUNT
     */
    public int getNumberOfThreads() {
        return numberOfThreads.orElse(DEFAULT_THREAD_COUNT);
    }

    /**
     * Sets the number of worker threads.
     * @param newValue the new value
     */
    public void setNumberOfThreads(int newValue) {
        this.numberOfThreads = OptionalInt.of(newValue);
    }

    /**
     * Returns the number of partitions.
     * @return the number of partitions
     * @see #KEY_PARTITION_COUNT
     */
    public int getNumberOfPartitions() {
        return numberOfPartitions.orElse(getNumberOfThreads());
    }

    /**
     * Sets the number of partitions.
     * @param newValue the new value
     */
    public void setNumberOfPartitions(int newValue) {
        this.numberOfPartitions = OptionalInt.of(newValue);
    }

    /**
     * Returns the maximum buffer pool size.
     * @return the maximum buffer pool size, in bytes
     * @see #KEY_BUFFER_POOL_SIZE
     */
    public long getBufferPoolSize() {
        return bufferPoolSize.orElse(DEFAULT_BUFFER_POOL_SIZE);
    }

    /**
     * Sets the maximum buffer pool size.
     * @param newValue the new value
     */
    public void setBufferPoolSize(long newValue) {
        this.bufferPoolSize = OptionalLong.of(newValue);
    }

    /**
     * Returns the buffer swap directory.
     * @return the buffer swap directory
     * @see #KEY_SWAP_DIRECTORY
     */
    public File getSwapDirectory() {
        return swapDirectory.orElse(DEFAULT_SWAP_DIRECTORY);
    }

    /**
     * Sets the buffer swap directory.
     * @param newValue the new value
     */
    public void setSwapDirectory(File newValue) {
        this.swapDirectory = Optional.ofNullable(newValue);
    }

    /**
     * Returns the swap division.
     * @return the swap division
     * @since 0.4.1
     */
    public int getSwapDivision() {
        return swapDivision.orElse(DEFAULT_SWAP_DIVISION);
    }

    /**
     * Sets the swap division.
     * @param newValue the swap division
     * @since 0.4.1
     */
    public void setSwapDivision(int newValue) {
        this.swapDivision = OptionalInt.of(newValue);
    }

    /**
     * Sets the swap file decorator.
     * @param newValue the decorator class name
     */
    public void setSwapDecorator(String newValue) {
        setSwapDecorator(SupplierInfo.of(newValue));
    }

    /**
     * Sets the swap file decorator.
     * @param newValue the decorator class supplier
     */
    public void setSwapDecorator(SupplierInfo newValue) {
        this.swapDecorator = Optional.ofNullable(newValue);
    }

    /**
     * Returns the swap file decorator.
     * @return the swap file decorator class information
     */
    public SupplierInfo getSwapDecorator() {
        return swapDecorator.orElse(DEFAULT_SWAP_DECORATOR);
    }

    /**
     * Returns the swap file decorator.
     * @param loader the class loader
     * @return the swap file decorator
     */
    public ByteChannelDecorator getSwapDecorator(ClassLoader loader) {
        Supplier<?> supplier = getSwapDecorator().newInstance(loader);
        return (ByteChannelDecorator) supplier.get();
    }

    /**
     * Returns the individual output buffer size.
     * @return the output buffer size, in bytes
     * @see #KEY_OUTPUT_BUFFER_SIZE
     */
    public int getOutputBufferSize() {
        return outputBufferSize.orElse(DEFAULT_OUTPUT_BUFFER_SIZE);
    }

    /**
     * Sets the individual output buffer size.
     * @param newValue the new value
     */
    public void setOutputBufferSize(int newValue) {
        this.outputBufferSize = OptionalInt.of(newValue);
    }

    /**
     * Returns the individual output buffer margin size.
     * @return the output buffer margin size, in bytes
     * @see #KEY_OUTPUT_BUFFER_MARGIN
     * @since 0.5.3
     */
    public int getOutputBufferMargin() {
        return outputBufferMargin.orElse(DEFAULT_OUTPUT_BUFFER_MARGIN);
    }

    /**
     * Sets the individual output buffer margin size.
     * @param newValue the new value
     * @since 0.5.3
     */
    public void setOutputBufferMargin(int newValue) {
        this.outputBufferMargin = OptionalInt.of(newValue);
    }

    /**
     * Returns the estimated average output record size.
     * @return the estimated average output record size
     * @see #KEY_OUTPUT_RECORD_SIZE
     */
    public int getOutputRecordSize() {
        return outputRecordSize.orElse(DEFAULT_OUTPUT_RECORD_SIZE);
    }

    /**
     * Sets the estimated average output record size.
     * @param newValue the new value
     */
    public void setOutputRecordSize(int newValue) {
        this.outputRecordSize = OptionalInt.of(newValue);
    }

    /**
     * Returns the recommended number of records in individual output buffer page.
     * @return the number of records
     */
    public int getNumberOfOutputRecords() {
        int bufferSize = getOutputBufferSize();
        int recordSize = Math.max(getOutputRecordSize(), 1);
        int estimated = bufferSize / recordSize;
        return Math.max(estimated, 1);
    }

    /**
     * Sets the max number of merging scatter/gather input chunks at one time.
     * @param newValue the new value
     * @since 0.5.3
     */
    public void setMergeThreshold(int newValue) {
        this.mergeThreshold = OptionalInt.of(newValue);
    }

    /**
     * Returns the max number of merging scatter/gather input chunks at one time.
     * @return the max number of merging scatter/gather input chunks, or {@code 0} to disabled
     * @since 0.5.3
     */
    public int getMergeThreshold() {
        return mergeThreshold.orElse(DEFAULT_MERGE_THRESHOLD);
    }

    /**
     * Sets the factor of the number of merging scatter/gather input chunks if its limit was exceeded.
     * @param newValue the number of merging scatter/gather input chunks
     * @since 0.5.3
     */
    public void setMergeFactor(double newValue) {
        this.mergeFactor = OptionalDouble.of(newValue);
    }

    /**
     * Returns the factor of the number of merging scatter/gather input chunks if its limit was exceeded.
     * @return the factor of the number of merging scatter/gather input chunks
     * @since 0.5.3
     */
    public double getMergeFactor() {
        return mergeFactor.orElse(DEFAULT_MERGE_FACTOR);
    }

    /**
     * Extracts configurations from the given options.
     * @param options the options
     * @return the extracted configuration
     */
    public static VanillaConfiguration extract(Function<String, Optional<String>> options) {
        Arguments.requireNonNull(options);
        VanillaConfiguration conf = new VanillaConfiguration();
        configureInt(conf::setNumberOfThreads, options, KEY_THREAD_COUNT);
        configureInt(conf::setNumberOfPartitions, options, KEY_PARTITION_COUNT);
        configureLong(conf::setBufferPoolSize, options, KEY_BUFFER_POOL_SIZE);
        configureFile(conf::setSwapDirectory, options, KEY_SWAP_DIRECTORY);
        configureInt(conf::setSwapDivision, options, KEY_SWAP_DIVISION);
        configureString(conf::setSwapDecorator, options, KEY_SWAP_DECORATOR);
        configureInt(conf::setOutputBufferSize, options, KEY_OUTPUT_BUFFER_SIZE);
        configureInt(conf::setOutputBufferMargin, options, KEY_OUTPUT_BUFFER_MARGIN);
        configureInt(conf::setOutputRecordSize, options, KEY_OUTPUT_RECORD_SIZE);
        configureInt(conf::setMergeThreshold, options, KEY_MERGE_THRESHOLD);
        configureDouble(conf::setMergeFactor, options, KEY_MERGE_FACTOR);
        if (LOG.isDebugEnabled()) {
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_THREAD_COUNT, conf.getNumberOfThreads()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_PARTITION_COUNT, conf.getNumberOfPartitions()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_BUFFER_POOL_SIZE, conf.getBufferPoolSize()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_OUTPUT_BUFFER_SIZE, conf.getOutputBufferSize()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_OUTPUT_BUFFER_MARGIN, conf.getOutputBufferMargin()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_OUTPUT_RECORD_SIZE, conf.getOutputRecordSize()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_SWAP_DIRECTORY, Optionals.of(conf.getSwapDirectory())
                        .map(File::getAbsolutePath)
                        .orElse("N/A"))); //$NON-NLS-1$
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_SWAP_DIVISION, conf.getSwapDivision()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_SWAP_DECORATOR, conf.getSwapDecorator()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_MERGE_THRESHOLD, conf.getMergeThreshold()));
            LOG.debug(MessageFormat.format("{0}: {1}", //$NON-NLS-1$
                    KEY_MERGE_FACTOR, conf.getMergeFactor()));
        }
        return conf;
    }

    private static void configureInt(IntConsumer target, Function<String, Optional<String>> opts, String key) {
        opts.apply(key)
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .map(value -> Arguments.safe(() -> Integer.parseInt(value), () -> MessageFormat.format(
                        "{0} must be an integer: {1}",
                        key, value)))
                .ifPresent(target::accept);
    }

    private static void configureLong(LongConsumer target, Function<String, Optional<String>> opts, String key) {
        opts.apply(key)
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .map(value -> Arguments.safe(() -> Long.parseLong(value), () -> MessageFormat.format(
                        "{0} must be an integer: {1}",
                        key, value)))
                .ifPresent(target::accept);
    }

    private static void configureDouble(DoubleConsumer target, Function<String, Optional<String>> opts, String key) {
        opts.apply(key)
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .map(value -> Arguments.safe(() -> Double.parseDouble(value), () -> MessageFormat.format(
                        "{0} must be an number: {1}",
                        key, value)))
                .ifPresent(target::accept);
    }

    private static void configureFile(Consumer<File> target, Function<String, Optional<String>> opts, String key) {
        opts.apply(key)
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .map(File::new)
                .ifPresent(target::accept);
    }

    private static void configureString(Consumer<String> target, Function<String, Optional<String>> opts, String key) {
        opts.apply(key)
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .ifPresent(target::accept);
    }
}