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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static final Logger LOG = LoggerFactory.getLogger(BasicExternalInputEstimator.class);

    private static final Map<ExternalInputInfo.DataSize, Double> DEFAULT_SIZE_MAP;
    static {
        Map<ExternalInputInfo.DataSize, Double> map = new LinkedHashMap<>();
        final int mega = 1024 * 1024;
        map.put(ExternalInputInfo.DataSize.TINY, 10.0 * mega);
        map.put(ExternalInputInfo.DataSize.SMALL, 200.0 * mega);
        map.put(ExternalInputInfo.DataSize.LARGE, Double.POSITIVE_INFINITY);
        DEFAULT_SIZE_MAP = EnumUtil.freeze(map);
    }

    /**
     * The compiler option key prefix of the size scale for the target data size.
     */
    public static final String PREFIX_KEY = "input.estimator."; //$NON-NLS-1$

    private static final Map<ExternalInputInfo.DataSize, String> KEY_SIZE_MAP;
    static {
        Map<ExternalInputInfo.DataSize, String> map = new LinkedHashMap<>();
        for (ExternalInputInfo.DataSize size : ExternalInputInfo.DataSize.values()) {
            map.put(size, PREFIX_KEY + size.name().toLowerCase(Locale.ENGLISH));
        }
        KEY_SIZE_MAP = EnumUtil.freeze(map);
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
            double size = estimate(context, operator.getInfo());
            if (Double.isNaN(size) == false) {
                OperatorEstimators.putSize(context, operator, size);
            }
        }
    }

    private double estimate(Context context, ExternalInputInfo info) {
        ExternalInputInfo.DataSize symbol = info.getDataSize();
        // find override
        String string = context.getOptions().get(KEY_SIZE_MAP.get(symbol), null);
        if (string != null) {
            LOG.debug("found custom estimator: DataSize.{} => {}", symbol, string); //$NON-NLS-1$
            try {
                double value = Double.parseDouble(string);
                return value;
            } catch (NumberFormatException e) {
                LOG.warn(MessageFormat.format(
                        "invalid custom estimator size: {0}={1}",
                        KEY_SIZE_MAP.get(symbol),
                        string), e);
            }
        }
        Double size = sizeMap.get(symbol);
        if (size == null) {
            return OperatorEstimate.UNKNOWN_SIZE;
        }
        return size;
    }
}
