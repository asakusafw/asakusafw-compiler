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
package com.asakusafw.dag.runtime.testing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.runtime.value.IntOption;

/**
 * Utilities of {@link MockDataModelUtil}.
 */
public final class MockDataModelUtil {

    private static final Comparator<MockDataModel> COMPARATOR = Comparator
            .comparing(MockDataModel::getKeyOption)
            .thenComparing(MockDataModel::getSortOption)
            .thenComparing(MockDataModel::getValueOption);

    private MockDataModelUtil() {
        return;
    }

    /**
     * Creates a new object.
     * @param key the key
     * @param sort the sort
     * @param value the value
     * @return the created object
     */
    public static MockDataModel object(int key, long sort, String value) {
        return new MockDataModel(key, BigDecimal.valueOf(sort), value);
    }

    /**
     * Creates a new object.
     * @param key the key
     * @param sort the sort
     * @param value the value
     * @return the created object
     */
    public static MockDataModel object(int key, String sort, String value) {
        return new MockDataModel(key, sort == null ? null : new BigDecimal(sort), value);
    }

    /**
     * Returns the sorted collection.
     * @param models the original collection
     * @return the sorted collection
     */
    public static List<MockDataModel> sort(Collection<? extends MockDataModel> models) {
        return models.stream()
                .sorted(COMPARATOR)
                .collect(Collectors.toList());
    }

    /**
     * {@link ValueSerDe}.
     */
    public static class SerDe implements ValueSerDe {

        private final MockDataModel buffer = new MockDataModel();

        @Override
        public void serialize(Object object, DataOutput output) throws IOException, InterruptedException {
            MockDataModel model = (MockDataModel) object;
            ValueOptionSerDe.serialize(model.getKeyOption(), output);
            ValueOptionSerDe.serialize(model.getSortOption(), output);
            ValueOptionSerDe.serialize(model.getValueOption(), output);
        }

        @Override
        public Object deserialize(DataInput input) throws IOException, InterruptedException {
            MockDataModel model = buffer;
            ValueOptionSerDe.deserialize(buffer.getKeyOption(), input);
            ValueOptionSerDe.deserialize(buffer.getSortOption(), input);
            ValueOptionSerDe.deserialize(buffer.getValueOption(), input);
            return model;
        }
    }

    /**
     * {@link KeyValueSerDe} + {@link DataComparator} - {@code =key, +sort, ...}.
     */
    public static class KvSerDe1 implements KeyValueSerDe, DataComparator {

        private final MockDataModel buffer = new MockDataModel();

        @Override
        public void serializeKey(Object object, DataOutput output) throws IOException, InterruptedException {
            MockDataModel model = (MockDataModel) object;
            ValueOptionSerDe.serialize(model.getKeyOption(), output);
        }

        @Override
        public void serializeValue(Object object, DataOutput output) throws IOException, InterruptedException {
            MockDataModel model = (MockDataModel) object;
            ValueOptionSerDe.serialize(model.getSortOption(), output);
            ValueOptionSerDe.serialize(model.getValueOption(), output);
        }

        @Override
        public Object deserializeKey(DataInput keyInput) throws IOException, InterruptedException {
            IntOption option = new IntOption();
            ValueOptionSerDe.deserialize(option, keyInput);
            return option.isNull() ? null : option.get();
        }

        @Override
        public Object deserializePair(DataInput keyInput, DataInput valueInput)
                throws IOException, InterruptedException {
            MockDataModel model = buffer;
            ValueOptionSerDe.deserialize(buffer.getKeyOption(), keyInput);
            ValueOptionSerDe.deserialize(buffer.getSortOption(), valueInput);
            ValueOptionSerDe.deserialize(buffer.getValueOption(), valueInput);
            return model;
        }

        @Override
        public int compare(DataInput a, DataInput b) throws IOException {
            return ValueOptionSerDe.compareDecimal(a, b);
        }
    }
}
