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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClassifierBinding;
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
 * An implementation of {@link OperatorClassifierBinding} for built-in operators.
 */
public class BuiltInOperatorClassifier
        extends OperatorClassifierBinding
        implements OperatorCharacterizer<OperatorClass> {

    private static final Map<ClassDescription, OperatorCharacterizer<? extends OperatorClass>> ENGINE_MAP;
    static {
        Map<ClassDescription, OperatorCharacterizer<? extends OperatorClass>> map = new HashMap<>();

        // extract kind
        map.put(classOf(Checkpoint.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Project.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Extend.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Restructure.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Branch.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Convert.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Extract.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Update.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Logging.class), new ExtractKindOperatorClassifier());
        map.put(classOf(Split.class), new ExtractKindOperatorClassifier());

        // aggregation kind
        map.put(classOf(Fold.class), new AggregationOperatorClassifier());
        map.put(classOf(Summarize.class), new AggregationOperatorClassifier());

        // master join kind
        map.put(classOf(MasterBranch.class), new MasterJoinOperatorClassifier());
        map.put(classOf(MasterCheck.class), new MasterJoinOperatorClassifier());
        map.put(classOf(MasterJoin.class), new MasterJoinOperatorClassifier());
        map.put(classOf(MasterJoinUpdate.class), new MasterJoinOperatorClassifier());

        // co-group kind
        map.put(classOf(GroupSort.class), new CoGroupKindOperatorClassifier());
        map.put(classOf(CoGroup.class), new CoGroupKindOperatorClassifier());

        ENGINE_MAP = map;
    }

    @Override
    public Collection<ClassDescription> getTargetOperators() {
        return ENGINE_MAP.keySet();
    }

    @Override
    public OperatorCharacterizer<OperatorClass> getEngine() {
        return this;
    }

    @Override
    public OperatorClass extract(Context context, Operator operator) {
        ClassDescription type = Util.getAnnotationType(operator);
        OperatorCharacterizer<? extends OperatorClass> delegate = ENGINE_MAP.get(type);
        assert delegate != null;
        return delegate.extract(context, operator);
    }
}
