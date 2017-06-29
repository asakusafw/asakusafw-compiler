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
package com.asakusafw.bridge.hadoop.directio;

import static com.asakusafw.bridge.hadoop.directio.Util.*;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.hadoop.ModelInputRecordReader;
import com.asakusafw.bridge.hadoop.combine.CombinedInputSplit;
import com.asakusafw.bridge.hadoop.combine.CombinedRecordReader;
import com.asakusafw.bridge.hadoop.combine.DefaultSplitCombiner;
import com.asakusafw.bridge.hadoop.combine.SplitCombiner;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceConstants;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.DirectInputFragment;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.directio.ResourcePattern;
import com.asakusafw.runtime.directio.SimpleDataDefinition;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.stage.input.BridgeInputFormat.NullInputSplit;
import com.asakusafw.runtime.stage.input.BridgeInputFormat.NullRecordReader;

/**
 * An Hadoop {@code InputFormat} for Direct I/O file inputs.
 * This requires {@link StageInfo} object onto Hadoop configuration as {@link StageInfo#KEY_NAME}.
 * @since 0.1.0
 * @version 0.4.2
 */
public class DirectFileInputFormat extends InputFormat<NullWritable, Object> {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileInputFormat.class);

    static final String KEY_PREFIX = "com.asakusafw.bridge.directio.input."; //$NON-NLS-1$

    /**
     * The attribute key name of base path.
     */
    public static final String KEY_BASE_PATH = KEY_PREFIX + "basePath"; //$NON-NLS-1$

    /**
     * The attribute key name of resource path/pattern.
     */
    public static final String KEY_RESOURCE_PATH = KEY_PREFIX + "resourcePath"; //$NON-NLS-1$

    /**
     * The attribute key name of data class.
     */
    public static final String KEY_DATA_CLASS = KEY_PREFIX + "dataClass"; //$NON-NLS-1$

    /**
     * The attribute key name of format class.
     */
    public static final String KEY_FORMAT_CLASS = KEY_PREFIX + "formatClass"; //$NON-NLS-1$

    /**
     * The attribute key name of filter class.
     */
    public static final String KEY_FILTER_CLASS = KEY_PREFIX + "filterClass"; //$NON-NLS-1$

    /**
     * The attribute key name of whether the target input is optional.
     */
    public static final String KEY_OPTIONAL = KEY_PREFIX + "optional"; //$NON-NLS-1$

    /**
     * The attribute key name of max number of combined input splits.
     * @since 0.4.2
     */
    public static final String KEY_COMBINE = KEY_PREFIX + "combine"; //$NON-NLS-1$

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        StageInfo stage = getStageInfo(conf);
        DirectFileInputInfo<?> info = extractInfo(context);
        DirectDataSourceRepository repository = getDataSourceRepository(context);
        String containerPath = repository.getContainerPath(info.basePath);
        List<DirectInputFragment> fragments = findFragments(info, repository);
        List<InputSplit> results = new ArrayList<>();
        for (DirectInputFragment fragment : fragments) {
            DirectFileInputSplit split = new DirectFileInputSplit(
                    containerPath, info.definition, fragment,
                    stage.getBatchArguments());
            ReflectionUtils.setConf(split, conf);
            results.add(split);
        }
        if (results.isEmpty()) {
            results.add(new NullInputSplit());
        }
        int splits = context.getConfiguration().getInt(KEY_COMBINE, -1);
        if (splits >= 1 && splits < results.size()) {
            SplitCombiner combiner = new DefaultSplitCombiner();
            return combiner.combine(context, splits, results);
        } else {
            return results;
        }
    }

    private static List<DirectInputFragment> findFragments(
            DirectFileInputInfo<?> info,
            DirectDataSourceRepository repository) throws IOException, InterruptedException {
        String containerPath = repository.getContainerPath(info.basePath);
        String componentPath = repository.getComponentPath(info.basePath);
        DirectDataSource ds = repository.getRelatedDataSource(containerPath);
        List<DirectInputFragment> fragments =
                ds.findInputFragments(info.definition, componentPath, info.resourcePattern);
        if (fragments.isEmpty()) {
            if (info.optional) {
                LOG.info(MessageFormat.format(
                        "skipped optional input (datasource={0}, path=\"{1}\", type={2})",
                        repository.getRelatedId(info.basePath),
                        ds.path(componentPath, info.resourcePattern),
                        info.definition.getDataClass().getName()));
            } else {
                throw new IOException(MessageFormat.format(
                        "input not found (datasource={0}, path=\"{1}\", type={2})",
                        repository.getRelatedId(info.basePath),
                        ds.path(componentPath, info.resourcePattern),
                        info.definition.getDataClass().getName()));
            }
        }
        return fragments;
    }

    private static DirectFileInputInfo<?> extractInfo(JobContext context) {
        Configuration conf = context.getConfiguration();
        String basePath = extract(conf, KEY_BASE_PATH, true, true);
        String resourcePath = extract(conf, KEY_RESOURCE_PATH, true, true);
        Class<?> dataClass = extractClass(conf, KEY_DATA_CLASS, true);
        Class<?> formatClass = extractClass(conf, KEY_FORMAT_CLASS, true);
        Class<?> filterClass = extractClass(conf, KEY_FILTER_CLASS, false);
        String optionalString = extract(conf, KEY_OPTIONAL, false, false);
        if (optionalString == null) {
            optionalString = DirectDataSourceConstants.DEFAULT_OPTIONAL;
        }

        ResourcePattern resourcePattern = FilePattern.compile(resourcePath);
        DataDefinition<?> definition = SimpleDataDefinition.newInstance(
                dataClass,
                (DataFormat<?>) ReflectionUtils.newInstance(formatClass, conf),
                createFilter(filterClass, conf));
        boolean optional = Boolean.parseBoolean(optionalString);

        return new DirectFileInputInfo<>(basePath, resourcePattern, definition, optional);
    }

    private static String extract(Configuration conf, String key, boolean mandatory, boolean resolve) {
        String value = conf.get(key);
        if (value == null) {
            if (mandatory) {
                throw new IllegalStateException(MessageFormat.format(
                        "missing mandatory configuration: {0}",
                        key));
            }
            return null;
        }
        if (resolve) {
            StageInfo info = getStageInfo(conf);
            try {
                value = info.resolveUserVariables(value);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(MessageFormat.format(
                        "failed to resolve configuration: {0}={1}",
                        key,
                        value), e);
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> extractClass(Configuration conf, String key, boolean mandatory) {
        String value = extract(conf, key, mandatory, false);
        if (value == null) {
            return null;
        }
        try {
            return (Class<T>) conf.getClassByName(value);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "failed to resolve a class: {0}={1}",
                    key,
                    value), e);
        }
    }

    @Override
    public RecordReader<NullWritable, Object> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof DirectFileInputSplit) {
            DirectFileInputSplit info = (DirectFileInputSplit) split;
            DataDefinition<?> definition = info.getDataDefinition();
            return createRecordReader(definition, info, context);
        } else if (split instanceof CombinedInputSplit) {
            return new CombinedRecordReader<>(this);
        } else if (split instanceof NullInputSplit) {
            return createNullRecordReader(context);
        } else {
            throw new IOException(MessageFormat.format(
                    "unknown input split: {0}",
                    split));
        }
    }

    private static <T> RecordReader<NullWritable, Object> createRecordReader(
            DataDefinition<T> definition,
            DirectFileInputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        assert definition != null;
        assert split != null;
        assert context != null;
        Configuration conf = context.getConfiguration();
        T buffer = ReflectionUtils.newInstance(definition.getDataClass(), conf);
        Counter counter = new Counter();
        DirectInputFragment fragment = split.getInputFragment();
        ModelInput<T> input = createInput(context, split.getContainerPath(), definition, counter, fragment);
        return new ModelInputRecordReader<>(input, buffer, counter, fragment.getSize());
    }

    private static <T> ModelInput<T> createInput(
            TaskAttemptContext context,
            String containerPath,
            DataDefinition<T> definition,
            Counter counter,
            DirectInputFragment fragment) throws IOException, InterruptedException {
        assert context != null;
        assert containerPath != null;
        assert definition != null;
        assert counter != null;
        assert fragment != null;
        DirectDataSourceRepository repo = getDataSourceRepository(context);
        DirectDataSource ds = repo.getRelatedDataSource(containerPath);
        return ds.openInput(definition, fragment, counter);
    }

    private static RecordReader<NullWritable, Object> createNullRecordReader(TaskAttemptContext context) {
        assert context != null;
        return new NullRecordReader<>();
    }

    private static class DirectFileInputInfo<T> {

        final String basePath;

        final ResourcePattern resourcePattern;

        final DataDefinition<T> definition;

        final boolean optional;

        DirectFileInputInfo(
                String basePath, ResourcePattern resourcePattern,
                DataDefinition<T> definition, boolean optional) {
            this.basePath = basePath;
            this.resourcePattern = resourcePattern;
            this.definition = definition;
            this.optional = optional;
        }
    }
}
