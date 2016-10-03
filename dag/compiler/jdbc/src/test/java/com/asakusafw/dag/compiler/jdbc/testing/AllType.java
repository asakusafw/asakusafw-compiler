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
package com.asakusafw.dag.compiler.jdbc.testing;

import java.lang.reflect.Method;

import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;

@SuppressWarnings("javadoc")
public class AllType implements DataModel<AllType> {

    private final BooleanOption booleanOption = new BooleanOption();
    public BooleanOption getBooleanOption() {
        return booleanOption;
    }

    private final ByteOption byteOption = new ByteOption();
    public ByteOption getByteOption() {
        return byteOption;
    }

    private final ShortOption shortOption = new ShortOption();
    public ShortOption getShortOption() {
        return shortOption;
    }

    private final IntOption intOption = new IntOption();
    public IntOption getIntOption() {
        return intOption;
    }

    private final LongOption longOption = new LongOption();
    public LongOption getLongOption() {
        return longOption;
    }

    private final FloatOption floatOption = new FloatOption();
    public FloatOption getFloatOption() {
        return floatOption;
    }

    private final DoubleOption doubleOption = new DoubleOption();
    public DoubleOption getDoubleOption() {
        return doubleOption;
    }

    private final DecimalOption decimalOption = new DecimalOption();
    public DecimalOption getDecimalOption() {
        return decimalOption;
    }

    private final StringOption stringOption = new StringOption();
    public StringOption getStringOption() {
        return stringOption;
    }

    private final DateOption dateOption = new DateOption();
    public DateOption getDateOption() {
        return dateOption;
    }

    private final DateTimeOption datetimeOption = new DateTimeOption();
    public DateTimeOption getDateTimeOption() {
        return datetimeOption;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void reset() {
        try {
            for (Method method : getClass().getMethods()) {
                if (method.getParameterCount() != 0
                        || ValueOption.class.isAssignableFrom(method.getReturnType()) == false) {
                    continue;
                }
                ValueOption<?> value = (ValueOption<?>) method.invoke(this);
                value.setNull();
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings({ "unchecked", "deprecation", "rawtypes" })
    @Override
    public void copyFrom(AllType other) {
        try {
            for (Method method : getClass().getMethods()) {
                if (method.getParameterCount() != 0
                        || ValueOption.class.isAssignableFrom(method.getReturnType()) == false) {
                    continue;
                }
                ValueOption from = (ValueOption<?>) method.invoke(other);
                ValueOption to = (ValueOption<?>) method.invoke(this);
                to.copyFrom(from);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
