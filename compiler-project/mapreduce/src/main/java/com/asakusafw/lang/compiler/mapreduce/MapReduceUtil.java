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
package com.asakusafw.lang.compiler.mapreduce;

import org.apache.hadoop.fs.Path;

import com.asakusafw.runtime.stage.StageOutput;

/**
 * Utilities for MapReduce operations.
 */
public final class MapReduceUtil {

    private MapReduceUtil() {
        return;
    }

    /**
     * Returns a quoted name for {@link StageOutput}.
     * @param name the output name
     * @return the processed name
     */
    public static String quoteOutputName(String name) {
        StringBuilder buf = new StringBuilder();
        for (char c : name.toCharArray()) {
            // 0 as escape character
            if ('1' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z') {
                buf.append(c);
            } else if (c <= 0xff) {
                buf.append('0');
                buf.append(String.format("%02x", (int) c)); //$NON-NLS-1$
            } else {
                buf.append("0u"); //$NON-NLS-1$
                buf.append(String.format("%04x", (int) c)); //$NON-NLS-1$
            }
        }
        return buf.toString();
    }

    /**
     * Returns the actual path of the target stage output.
     * @param basePath the stage output base path
     * @param outputName the stage output name (see {@link #quoteOutputName(String)})
     * @return the processed path
     */
    public static String getStageOutputPath(String basePath, String outputName) {
        String fileName = String.format("%s-*", outputName); //$NON-NLS-1$
        Path path = new Path(basePath, fileName);
        return path.toString();
    }
}
