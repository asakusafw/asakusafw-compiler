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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.asakusafw.bridge.hadoop.ConfigurationEditor;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.DirectInputFragment;
import com.asakusafw.runtime.directio.SimpleDataDefinition;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;

/**
 * Utilities for this package.
 */
public final class Util {

    private Util() {
        return;
    }

    static DirectDataSourceRepository getDataSourceRepository(JobContext context) {
        return HadoopDataSourceUtil.loadRepository(context.getConfiguration());
    }

    static DataFilter<?> createFilter(
            Class<?> filterClass, Configuration configuration) {
        if (filterClass == null) {
            return null;
        }
        Map<String, String> batchArguments = getStageInfo(configuration).getBatchArguments();
        return createFilter(filterClass, batchArguments, configuration);
    }

    static DataFilter<?> createFilter(
            Class<?> filterClass, Map<String, String> batchArguments, Configuration configuration) {
        if (filterClass == null) {
            return null;
        }
        DataFilter<?> result = (DataFilter<?>) ReflectionUtils.newInstance(filterClass, configuration);
        DataFilter.Context context = new DataFilter.Context(batchArguments);
        result.initialize(context);
        return result;
    }

    static StageInfo getStageInfo(Configuration configuration) {
        StageInfo info = ConfigurationEditor.findStageInfo(configuration);
        if (info == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "failed to extract StageInfo object: {0}",
                    ConfigurationEditor.KEY_STAGE_INFO));
        }
        return info;
    }

    static void writeDataDefinition(DataOutput out, DataDefinition<?> definition) throws IOException {
        Text.writeString(out, definition.getDataClass().getName());
        Text.writeString(out, definition.getDataFormat().getClass().getName());
        if (definition.getDataFilter() == null) {
            Text.writeString(out, ""); //$NON-NLS-1$
        } else {
            Text.writeString(out, definition.getDataFilter().getClass().getName());
        }
    }

    static DataDefinition<?> readDataDefinition(
            DataInput in, Map<String, String> batchArguments, Configuration conf) throws IOException {
        Class<?> data;
        DataFormat<?> format;
        DataFilter<?> filter;
        try {
            data = conf.getClassByName(Text.readString(in));
            format = (DataFormat<?>) ReflectionUtils.newInstance(conf.getClassByName(Text.readString(in)), conf);
            String filterClass = Text.readString(in);
            if (filterClass.isEmpty()) {
                filter = null;
            } else {
                filter = createFilter(conf.getClassByName(filterClass), batchArguments, conf);
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("error occurred while extracting data definition", e);
        }
        return SimpleDataDefinition.newInstance(data, format, filter);
    }

    static void writeFragment(DataOutput out, DirectInputFragment fragment) throws IOException {
        WritableUtils.writeString(out, fragment.getPath());
        WritableUtils.writeVLong(out, fragment.getOffset());
        WritableUtils.writeVLong(out, fragment.getSize());
        List<String> ownerNodeNames = fragment.getOwnerNodeNames();
        WritableUtils.writeStringArray(out, ownerNodeNames.toArray(new String[ownerNodeNames.size()]));
        Map<String, String> attributes = fragment.getAttributes();
        writeMap(out, attributes);
    }

    static DirectInputFragment readFragment(DataInput in) throws IOException {
        String path = WritableUtils.readString(in);
        long offset = WritableUtils.readVLong(in);
        long length = WritableUtils.readVLong(in);
        String[] locations = WritableUtils.readStringArray(in);
        Map<String, String> attributes = readMap(in);
        return new DirectInputFragment(path, offset, length, Arrays.asList(locations), attributes);
    }

    static void writeMap(DataOutput out, Map<String, String> map) throws IOException {
        if (map == null) {
            WritableUtils.writeVInt(out, 0);
        } else {
            WritableUtils.writeVInt(out, map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }
    }

    static Map<String, String> readMap(DataInput in) throws IOException {
        int size = WritableUtils.readVInt(in);
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            String k = Text.readString(in);
            String v = Text.readString(in);
            result.put(k, v);
        }
        return result;
    }
}
