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
package com.asakusafw.lang.compiler.extension.directio;

import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor.Context;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoPortProcessor.ResolvedInput;
import com.asakusafw.lang.compiler.hadoop.InputFormatInfo;
import com.asakusafw.lang.compiler.hadoop.InputFormatInfoSupport;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * {@link InputFormatInfoSupport} for Direct I/O file inputs.
 */
public class DirectFileInputFormatInfoSupport implements InputFormatInfoSupport {

    private static final ClassDescription FORMAT_CLASS =
            new ClassDescription("com.asakusafw.bridge.hadoop.directio.DirectFileInputFormat"); //$NON-NLS-1$

    private static final ClassDescription KEY_CLASS =
            new ClassDescription("org.apache.hadoop.io.NullWritable"); //$NON-NLS-1$

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

    @Override
    public InputFormatInfo resolveInput(Context context, String name, ExternalInputInfo info) {
        ResolvedInput resolved = DirectFileIoPortProcessor.restoreModel(context, name, info);
        DirectFileInputModel model = resolved.model;

        Map<String, String> extra = new LinkedHashMap<>();
        extra.put(KEY_BASE_PATH, model.getBasePath());
        extra.put(KEY_RESOURCE_PATH, model.getResourcePattern());
        extra.put(KEY_DATA_CLASS, info.getDataModelClass().getBinaryName());
        extra.put(KEY_FORMAT_CLASS, model.getFormatClass().getBinaryName());
        if (DirectFileIoPortProcessor.isFilterEnabled(context) && model.getFilterClass() != null) {
            extra.put(KEY_FILTER_CLASS, model.getFilterClass().getBinaryName());
        }
        extra.put(KEY_OPTIONAL, String.valueOf(model.isOptional()));
        return new InputFormatInfo(FORMAT_CLASS, KEY_CLASS, info.getDataModelClass(), extra);
    }
}
