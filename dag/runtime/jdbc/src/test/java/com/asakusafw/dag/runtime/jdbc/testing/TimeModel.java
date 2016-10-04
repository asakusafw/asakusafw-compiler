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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.ValueOption;

@SuppressWarnings({ "javadoc", "deprecation" })
public class TimeModel extends DataModelBase<TimeModel> {

    private final IntOption keyOption = new IntOption();

    private final DateOption dateValueOption = new DateOption();

    private final DateTimeOption timestampValueOption = new DateTimeOption();

    public TimeModel() {
        this(0, null, null);
    }

    public TimeModel(TimeModel copy) {
        copyFrom(copy);
    }

    public TimeModel(int key, Date date, DateTime timestamp) {
        keyOption.modify(key);
        dateValueOption.modify(date);
        timestampValueOption.modify(timestamp);
    }

    public IntOption getKeyOption() {
        return keyOption;
    }

    public DateOption getDateValueOption() {
        return dateValueOption;
    }

    public DateTimeOption getTimestampValueOption() {
        return timestampValueOption;
    }

    @Override
    protected List<Function<TimeModel, ? extends ValueOption<?>>> properties() {
        return Arrays.asList(TimeModel::getKeyOption, TimeModel::getDateValueOption, TimeModel::getTimestampValueOption);
    }
}
