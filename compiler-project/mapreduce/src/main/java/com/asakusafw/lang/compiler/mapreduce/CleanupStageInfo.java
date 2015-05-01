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
package com.asakusafw.lang.compiler.mapreduce;

/**
 * Represents a cleanup stage structure.
 * @see CleanupStageEmitter
 */
public class CleanupStageInfo {

    /**
     * The default cleanup stage ID.
     */
    public static final String DEFAULT_STAGE_ID = "cleanup"; //$NON-NLS-1$

    final StageInfo meta;

    final String cleanupPath;

    /**
     * Creates a new instance.
     * @param meta meta information for the stage
     * @param baseOutputPath the cleanup target path
     */
    public CleanupStageInfo(StageInfo meta, String baseOutputPath) {
        this.meta = meta;
        this.cleanupPath = baseOutputPath;
    }
}
