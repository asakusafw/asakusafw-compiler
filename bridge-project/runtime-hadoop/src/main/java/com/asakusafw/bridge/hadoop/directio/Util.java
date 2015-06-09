/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.asakusafw.bridge.hadoop.Compatibility;
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
        return HadoopDataSourceUtil.loadRepository(Compatibility.getConfiguration(context));
    }

    static DataFilter<?> createFilter(Class<?> filterClass, Configuration configuration) {
        if (filterClass == null) {
            return null;
        }
        DataFilter<?> result = (DataFilter<?>) ReflectionUtils.newInstance(filterClass, configuration);
        Map<String, String> batchArguments = getStageInfo(configuration).getBatchArguments();
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

    static DataDefinition<?> readDataDefinition(DataInput in, Configuration conf) throws IOException {
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
                filter = createFilter(conf.getClassByName(filterClass), conf);
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
        WritableUtils.writeVInt(out, attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            WritableUtils.writeString(out, entry.getKey());
            WritableUtils.writeString(out, entry.getValue());
        }
    }

    static DirectInputFragment readFragment(DataInput in) throws IOException {
        String path = WritableUtils.readString(in);
        long offset = WritableUtils.readVLong(in);
        long length = WritableUtils.readVLong(in);
        String[] locations = WritableUtils.readStringArray(in);
        Map<String, String> attributes;
        int attributeCount = WritableUtils.readVInt(in);
        if (attributeCount == 0) {
            attributes = Collections.emptyMap();
        } else {
            attributes = new HashMap<>();
            for (int i = 0; i < attributeCount; i++) {
                String key = WritableUtils.readString(in);
                String value = WritableUtils.readString(in);
                attributes.put(key, value);
            }
        }
        return new DirectInputFragment(path, offset, length, Arrays.asList(locations), attributes);
    }
}
