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
package com.asakusafw.dag.compiler.codegen;

import java.math.BigDecimal;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * Represents property type kind.
 * @since 0.4.0
 */
public enum PropertyTypeKind {

    /**
     * {@code boolean} type.
     */
    BOOLEAN(BooleanOption.class, boolean.class),

    /**
     * {@code byte} type.
     */
    BYTE(ByteOption.class, byte.class),

    /**
     * {@code short} type.
     */
    SHORT(ShortOption.class, short.class),

    /**
     * {@code int} type.
     */
    INT(IntOption.class, int.class),

    /**
     * {@code long} type.
     */
    LONG(LongOption.class, long.class),

    /**
     * {@code float} type.
     */
    FLOAT(FloatOption.class, float.class),

    /**
     * {@code double} type.
     */
    DOUBLE(DoubleOption.class, double.class),

    /**
     * {@code decimal} type.
     */
    DECIMAL(DecimalOption.class, BigDecimal.class),

    /**
     * {@code string} type.
     */
    STRING(StringOption.class, Text.class),

    /**
     * {@code date} type.
     */
    DATE(DateOption.class, Date.class),

    /**
     * {@code date-time} type.
     */
    DATE_TIME(DateTimeOption.class, DateTime.class),
    ;

    private final ClassDescription optionType;

    private final TypeDescription rawType;

    PropertyTypeKind(Class<? extends ValueOption<?>> optionType, Class<?> rawType) {
        this.optionType = Descriptions.classOf(optionType);
        this.rawType = Descriptions.typeOf(rawType);
    }

    /**
     * Returns the {@link ValueOption} type for this.
     * @return the option type
     */
    public ClassDescription getOptionType() {
        return optionType;
    }

    /**
     * Returns the raw value type for this.
     * @return the raw value type
     */
    public TypeDescription getRawType() {
        return rawType;
    }

    /**
     * Returns the related kind for the {@link ValueOption} type.
     * @param type the {@link ValueOption} type
     * @return the related kind
     */
    public static PropertyTypeKind fromOptionType(TypeDescription type) {
        return Arguments.requireNonNull(Lazy.OPTION_TYPES.get(type), () -> type);

    }

    /**
     * Returns the related kind for the raw value type.
     * @param type the raw value type
     * @return the related kind
     */
    public static PropertyTypeKind fromRawType(TypeDescription type) {
        return Arguments.requireNonNull(Lazy.RAW_TYPES.get(type), () -> type);
    }

    private static final class Lazy {

        static final Map<ClassDescription, PropertyTypeKind> OPTION_TYPES = Stream.of(PropertyTypeKind.values())
                .collect(Collectors.toMap(PropertyTypeKind::getOptionType, Function.identity()));

        static final Map<TypeDescription, PropertyTypeKind> RAW_TYPES = Stream.of(PropertyTypeKind.values())
                .collect(Collectors.toMap(PropertyTypeKind::getRawType, Function.identity()));

        private Lazy() {
            return;
        }
    }
}
