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
package com.asakusafw.lang.compiler.optimizer.basic;

import com.asakusafw.lang.compiler.optimizer.OperatorEstimate;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;

/**
 * Basic implementations of optimizer facilities.
 */
public final class BasicOptimizers {

    private BasicOptimizers() {
        return;
    }

    /**
     * Returns a builder for the preset basic {@link OperatorEstimator}.
     * @param loader the class loader
     * @return the created builder
     */
    public static CompositeOperatorEstimator.Builder getDefaultEstimator(ClassLoader loader) {
        return CompositeOperatorEstimator.builder()
                .withInput(new BasicExternalInputEstimator())
                .withOutput(new BasicConstantEstimator(OperatorEstimate.UNKNOWN_SIZE))
                .withMarker(new BasicPropagateEstimator())
                .load(loader, OperatorEstimatorBinding.class);
    }

    /**
     * Returns a builder for the preset basic operator classifier.
     * @param loader the class loader
     * @return the created builder
     */
    public static CompositeOperatorCharacterizer.Builder<OperatorClass> getDefaultClassifier(ClassLoader loader) {
        return CompositeOperatorCharacterizer.<OperatorClass>builder()
                .withInput(new BasicExternalInputClassifier())
                .withOutput(new BasicExternalOutputClassifier())
                .withMarker(new BasicMarkerOperatorClassifier())
                .load(loader, OperatorClassifierBinding.class);
    }

    /**
     * Returns a builder for the preset basic operator rewriter.
     * @param loader the class loader
     * @return the created builder
     */
    public static CompositeOperatorRewriter.Builder getDefaultRewriter(ClassLoader loader) {
        return CompositeOperatorRewriter.builder()
                .load(loader, OperatorRewriter.class);
    }
}
