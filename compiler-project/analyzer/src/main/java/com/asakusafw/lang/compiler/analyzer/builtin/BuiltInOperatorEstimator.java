/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimators;
import com.asakusafw.lang.compiler.optimizer.basic.BasicPropagateEstimator;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorEstimatorBinding;
import com.asakusafw.vocabulary.operator.Branch;
import com.asakusafw.vocabulary.operator.Checkpoint;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Convert;
import com.asakusafw.vocabulary.operator.Extend;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Fold;
import com.asakusafw.vocabulary.operator.GroupSort;
import com.asakusafw.vocabulary.operator.Logging;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.MasterCheck;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.Project;
import com.asakusafw.vocabulary.operator.Restructure;
import com.asakusafw.vocabulary.operator.Split;
import com.asakusafw.vocabulary.operator.Summarize;
import com.asakusafw.vocabulary.operator.Update;

/**
 * An implementation of {@link OperatorEstimatorBinding} for built-in operators.
 */
public class BuiltInOperatorEstimator
        extends OperatorEstimatorBinding
        implements OperatorEstimator {

    static final Logger LOG = LoggerFactory.getLogger(BuiltInOperatorEstimator.class);

    /**
     * The compiler option key prefix of the size scale for the target operator (simple annotation type).
     */
    public static final String PREFIX_KEY = "operator.estimator."; //$NON-NLS-1$

    static final double PROJECT_SCALE = 1.0;

    static final double EXTEND_SCALE = 1.25;

    static final double UPDATE_SCALE = 2.0;

    static final double BRANCH_SCALE = 1.0;

    static final double FOLD_SCALE = 1.0;

    static final double JOIN_SCALE = 2.0;

    static final double JOIN_BRANCH_SCALE = 1.0;

    private static final int JOIN_INPUT = MasterJoin.ID_INPUT_TRANSACTION;

    private static final Map<ClassDescription, OperatorEstimator> ENGINE_MAP;
    static {
        Map<ClassDescription, OperatorEstimator> map = new HashMap<>();

        // identical
        map.put(classOf(Checkpoint.class), new BasicPropagateEstimator());
        map.put(classOf(Logging.class), new BasicPropagateEstimator());

        // branch-like
        map.put(classOf(Branch.class), new BasicPropagateEstimator(0, BRANCH_SCALE));

        // projection-like
        map.put(classOf(Project.class), new BasicPropagateEstimator(0, PROJECT_SCALE));
        map.put(classOf(Extend.class), new BasicPropagateEstimator(0, EXTEND_SCALE));
        map.put(classOf(Restructure.class), new BasicPropagateEstimator(0, EXTEND_SCALE));
        map.put(classOf(Split.class), new BasicPropagateEstimator(0, PROJECT_SCALE));

        // update-like
        map.put(classOf(Update.class), new BasicPropagateEstimator(0, UPDATE_SCALE));
        map.put(classOf(Convert.class), new BasicPropagateEstimator(0, UPDATE_SCALE));

        // fold-like
        map.put(classOf(Summarize.class), new BasicPropagateEstimator(0, FOLD_SCALE));
        map.put(classOf(Fold.class), new BasicPropagateEstimator(0, FOLD_SCALE));

        // join-like
        map.put(classOf(MasterJoin.class), new BasicPropagateEstimator(JOIN_INPUT, JOIN_SCALE));
        map.put(classOf(MasterJoinUpdate.class), new BasicPropagateEstimator(JOIN_INPUT, JOIN_SCALE));

        // join-branch-like
        map.put(classOf(MasterCheck.class), new BasicPropagateEstimator(JOIN_INPUT, JOIN_BRANCH_SCALE));
        map.put(classOf(MasterBranch.class), new BasicPropagateEstimator(JOIN_INPUT, JOIN_BRANCH_SCALE));

        // complex
        map.put(classOf(Extract.class), OperatorEstimator.NULL);
        map.put(classOf(CoGroup.class), OperatorEstimator.NULL);
        map.put(classOf(GroupSort.class), OperatorEstimator.NULL);

        ENGINE_MAP = map;
    }

    @Override
    public Collection<ClassDescription> getTargetOperators() {
        return ENGINE_MAP.keySet();
    }

    @Override
    public OperatorEstimator getEngine() {
        return this;
    }

    @Override
    public void perform(Context context, Operator operator) {
        OperatorEstimator delegate = findEngine(context, operator);
        assert delegate != null;
        delegate.perform(context, operator);
    }

    private OperatorEstimator findEngine(Context context, Operator operator) {
        ClassDescription type = Util.getAnnotationType(operator);
        String string = context.getOptions().get(toEngineKey(type), null);
        if (string != null) {
            LOG.debug("found custom estimator: {} => {}", type.getSimpleName(), string); //$NON-NLS-1$
            try {
                double scale = Double.parseDouble(string);
                if (Double.isNaN(scale)) {
                    return OperatorEstimator.NULL;
                }
                return new CustomEstimator(scale);
            } catch (NumberFormatException e) {
                LOG.warn(MessageFormat.format(
                        "invalid custom estimator scale: {0}={1}",
                        toEngineKey(type),
                        string), e);
            }
        }
        return ENGINE_MAP.get(type);
    }

    private String toEngineKey(ClassDescription type) {
        return PREFIX_KEY + type.getSimpleName();
    }

    private static class CustomEstimator implements OperatorEstimator {

        private final double scale;

        CustomEstimator(double scale) {
            this.scale = scale;
        }

        @Override
        public void perform(Context context, Operator operator) {
            double total = 0.0;
            for (OperatorInput input : operator.getInputs()) {
                double size = OperatorEstimators.getSize(context, input);
                if (Double.isNaN(size)) {
                    return;
                }
                total += size;
            }
            OperatorEstimators.putSize(context, operator, total * scale);
        }
    }
}
