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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.EnumMap;
import java.util.Map;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimators;

/**
 * Estimates size about {@link ExternalInput}.
 */
public class BasicExternalInputEstimator implements OperatorEstimator {

    private static final Map<ExternalInputInfo.DataSize, Double> DEFAULT_SIZE_MAP;
    static {
        Map<ExternalInputInfo.DataSize, Double> map = new EnumMap<>(ExternalInputInfo.DataSize.class);
        final int mega = 1024 * 1024;
        map.put(ExternalInputInfo.DataSize.TINY, 10.0 * mega);
        map.put(ExternalInputInfo.DataSize.SMALL, 200.0 * mega);
        map.put(ExternalInputInfo.DataSize.LARGE, Double.POSITIVE_INFINITY);
        DEFAULT_SIZE_MAP = map;
    }

    private final Map<ExternalInputInfo.DataSize, Double> sizeMap;

    /**
     * Creates a new instance.
     */
    public BasicExternalInputEstimator() {
        this(DEFAULT_SIZE_MAP);
    }

    /**
     * Creates a new instance.
     * @param sizeMap data size map
     */
    public BasicExternalInputEstimator(Map<ExternalInputInfo.DataSize, Double> sizeMap) {
        this.sizeMap = EnumUtil.freeze(sizeMap);
    }

    @Override
    public void perform(Context context, Operator operator) {
        if (operator instanceof ExternalInput) {
            perform(context, (ExternalInput) operator);
        }
    }

    private void perform(Context context, ExternalInput operator) {
        if (operator.isExternal()) {
            double size = estimate(operator.getInfo());
            if (Double.isNaN(size) == false) {
                OperatorEstimators.putSize(context, operator, size);
            }
        }
    }

    private double estimate(ExternalInputInfo info) {
        Double size = sizeMap.get(info.getDataSize());
        if (size == null) {
            return OperatorEstimate.UNKNOWN_SIZE;
        }
        return size;
    }
}
