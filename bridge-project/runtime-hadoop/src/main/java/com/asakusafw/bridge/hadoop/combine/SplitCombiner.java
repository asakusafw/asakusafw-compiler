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
package com.asakusafw.bridge.hadoop.combine;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Combines {@link InputSplit}.
 * @since 0.4.2
 */
public interface SplitCombiner {

    /**
     * Combines {@link InputSplit}.
     * @param context current context
     * @param maxSplits the max number of combined input splits
     * @param splits the original splits
     * @return the combined splits
     * @throws IOException if failed to combine by I/O error
     * @throws InterruptedException if interrupted while combine
     */
    List<InputSplit> combine(
            JobContext context,
            int maxSplits,
            Collection<? extends InputSplit> splits) throws IOException, InterruptedException;
}
