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
package com.asakusafw.bridge.hadoop;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * Utilities for Hadoop configuration.
 */
public final class ConfigurationEditor {

    /**
     * The key name for storing {@link StageInfo} object in serialized form.
     * @see StageInfo#serialize()
     */
    public static final String KEY_STAGE_INFO = StageInfo.KEY_NAME;

    private ConfigurationEditor() {
        return;
    }

    /**
     * Merge the extra configuration into the Hadoop configuration.
     * @param conf the target Hadoop configuration
     * @param extra the extra properties
     */
    public static void merge(Configuration conf, Map<String, String> extra) {
        for (Map.Entry<String, String> entry : extra.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Puts the {@link StageInfo} object into the Hadoop configuration.
     * @param conf the target Hadoop configuration
     * @param info the {@link StageInfo} object
     */
    public static void putStageInfo(Configuration conf, StageInfo info) {
        conf.set(KEY_STAGE_INFO, info.serialize());
    }

    /**
     * Returns the {@link StageInfo} object from the Hadoop configuration.
     * @param conf the target Hadoop configuration
     * @return the restored {@link StageInfo} object which is put using
     *     {@link #putStageInfo(Configuration, StageInfo)}, or {@code null} if it does not exist
     */
    public static StageInfo findStageInfo(Configuration conf) {
        String serialized = conf.get(KEY_STAGE_INFO);
        if (serialized == null) {
            return null;
        }
        return StageInfo.deserialize(serialized);
    }
}
