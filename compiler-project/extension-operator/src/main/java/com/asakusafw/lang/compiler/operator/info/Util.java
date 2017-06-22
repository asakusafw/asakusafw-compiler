/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.operator.info;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ArrayDescription;
import com.asakusafw.lang.compiler.model.description.ArrayTypeDescription;
import com.asakusafw.lang.compiler.model.description.BasicTypeDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.SerializableValueDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.UnknownValueDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.info.operator.InputGranularity;
import com.asakusafw.lang.info.operator.InputGroup;
import com.asakusafw.lang.info.value.AnnotationInfo;
import com.asakusafw.lang.info.value.ClassInfo;
import com.asakusafw.lang.info.value.EnumInfo;
import com.asakusafw.lang.info.value.ListInfo;
import com.asakusafw.lang.info.value.UnknownInfo;
import com.asakusafw.lang.info.value.ValueInfo;

final class Util {

    static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util() {
        return;
    }

    static ValueInfo convert(ValueDescription value) {
        switch (value.getValueKind()) {
        case IMMEDIATE:
            return convert((ImmediateDescription) value);
        case ENUM_CONSTANT:
            return convert((EnumConstantDescription) value);
        case ARRAY:
            return convert((ArrayDescription) value);
        case TYPE:
            return convert((TypeDescription) value);
        case ANNOTATION:
            return convert((AnnotationDescription) value);
        case SERIALIZABLE:
            return convert((SerializableValueDescription) value);
        case UNKNOWN:
            return convert((UnknownValueDescription) value);
        default:
            throw new AssertionError(value);
        }
    }

    static ValueInfo convert(ImmediateDescription value) {
        return ValueInfo.of(value.getValue());
    }

    static EnumInfo convert(EnumConstantDescription value) {
        return EnumInfo.of(
                convert(value.getDeclaringClass()),
                value.getName());
    }

    static ListInfo convert(ArrayDescription value) {
        return ListInfo.of(value.getElements().stream()
                .map(Util::convert)
                .collect(Collectors.toList()));
    }

    static ClassInfo convert(TypeDescription type) {
        switch (type.getTypeKind()) {
        case CLASS:
            return convert((ClassDescription) type);
        case BASIC:
            return ClassInfo.of(((BasicTypeDescription) type).getBasicTypeKind().getReflectiveObject());
        case ARRAY:
            return convert(((ArrayTypeDescription) type).getComponentType()).getArrayType();
        default:
            throw new AssertionError(type);
        }
    }

    static ClassInfo convert(ClassDescription type) {
        return ClassInfo.of(type.getName());
    }

    static AnnotationInfo convert(AnnotationDescription annotation) {
        Map<String, ValueInfo> elements = new LinkedHashMap<>();
        annotation.getElements().forEach((k, v) -> elements.put(k, convert(v)));
        return AnnotationInfo.of(
                convert(annotation.getDeclaringClass()),
                elements);
    }

    static ValueInfo convert(SerializableValueDescription value) {
        try {
            return ValueInfo.of(value.resolve(Util.class.getClassLoader()));
        } catch (ReflectiveOperationException e) {
            LOG.trace("error occurred while deserializing value: {}", value, e);
            return UnknownInfo.of(
                    convert(value.getValueType()),
                    "?");
        }
    }

    static ValueInfo convert(UnknownValueDescription value) {
        return UnknownInfo.of(
                convert(value.getValueType()),
                value.getLabel());
    }

    static InputGranularity translate(OperatorInput.InputUnit kind) {
        switch (kind) {
        case RECORD:
            return InputGranularity.RECORD;
        case GROUP:
            return InputGranularity.GROUP;
        case WHOLE:
            return InputGranularity.WHOLE;
        default:
            throw new AssertionError(kind);
        }
    }

    static InputGroup translate(Group group) {
        if (group == null) {
            return null;
        }
        return new InputGroup(
                group.getGrouping().stream()
                    .map(PropertyName::toName)
                    .collect(Collectors.toList()),
                group.getOrdering().stream()
                    .map(it -> new InputGroup.Order(it.getPropertyName().toName(), translate(it.getDirection())))
                    .collect(Collectors.toList()));
    }

    private static InputGroup.Direction translate(Group.Direction kind) {
        switch (kind) {
        case ASCENDANT:
            return InputGroup.Direction.ASCENDANT;
        case DESCENDANT:
            return InputGroup.Direction.DESCENDANT;
        default:
            throw new AssertionError(kind);
        }
    }
}
