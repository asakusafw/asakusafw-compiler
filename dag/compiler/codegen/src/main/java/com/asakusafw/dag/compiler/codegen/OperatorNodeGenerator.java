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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Generates classes for processing Asakusa operators.
 * This can handles only {@link CoreOperator} and {@link UserOperator}.
 * @since 0.4.0
 */
public interface OperatorNodeGenerator {

    /**
     * Returns the target annotation type.
     * @return the target annotation type
     */
    ClassDescription getAnnotationType();

    /**
     * Generates a class for processing the operator, and returns the generated class binary.
     * @param context the current context
     * @param operator the target operator
     * @param classNamer the target class namer
     * @return the generated node info
     */
    NodeInfo generate(Context context, Operator operator, Supplier<? extends ClassDescription> classNamer);

    /**
     * Represents a processing context of {@link OperatorNodeGenerator}.
     * @since 0.4.0
     */
    interface Context extends ClassGeneratorContext {

        /**
         * Returns the dependency element for the property.
         * @param property the target property
         * @return the dependency
         */
        VertexElement getDependency(OperatorProperty property);

        /**
         * Returns the dependency element for the properties.
         * @param properties the target properties
         * @return the dependencies
         */
        default List<VertexElement> getDependencies(OperatorProperty... properties) {
            return Lang.project(properties, e -> Invariants.requireNonNull(getDependency(e)));
        }

        /**
         * Returns the dependency element for the properties.
         * @param properties the target properties
         * @return the dependencies
         */
        default List<VertexElement> getDependencies(List<? extends OperatorProperty> properties) {
            return Lang.project(properties, e -> Invariants.requireNonNull(getDependency(e)));
        }

        /**
         * Returns the suggested name of the target property.
         * The name is a valid Java identifier, and is unique for the other suggested names.
         * @param property the target property
         * @return the suggested name
         */
        default String getSuggestedName(OperatorProperty property) {
            Arguments.requireNonNull(property);
            PropertyName name = PropertyName.of(property.getName())
                .addFirst(property.getPropertyKind().name().toLowerCase(Locale.ENGLISH));
            return name.toMemberName();
        }

        /**
         * Returns whether the input is a provided via a side-data channel or not.
         * @param input the target input
         * @return {@code true} if the input is side-data, otherwise {@code false}
         */
        default boolean isSideData(OperatorInput input) {
            return input.getInputUnit() == InputUnit.WHOLE;
        }

        /**
         * Returns the group index of the target input.
         * @param input the target input
         * @return the input ID, or {@code -1} if the input it omitted
         * @throws IllegalArgumentException if the input is not group, or it is a side-data
         */
        default int getGroupIndex(OperatorInput input) {
            if (input.getInputUnit() != InputUnit.GROUP) {
                return -1;
            }
            int index = 0;
            for (OperatorInput port : input.getOwner().getInputs()) {
                if (port.getInputUnit() == InputUnit.GROUP) {
                    if (port.equals(input)) {
                        return index;
                    }
                    index++;
                }
            }
            return -1;
        }
    }

    /**
     * Represents the node info generated by {@link OperatorNodeGenerator}.
     */
    interface NodeInfo {

        /**
         * Returns the class data.
         * @return the class data
         */
        ClassData getClassData();

        /**
         * Returns the data type.
         * @return the data type
         */
        TypeDescription getDataType();

        /**
         * Returns the dependencies.
         * @return the dependencies
         */
        List<VertexElement> getDependencies();
    }

    /**
     * Represents the node info generated by {@link OperatorNodeGenerator}.
     * @since 0.4.0
     */
    class OperatorNodeInfo implements NodeInfo {

        private final ClassData classData;

        private final TypeDescription dataType;

        private final List<VertexElement> dependencies;

        /**
         * Creates a new instance.
         * @param classData the generated class data
         * @param dataType the input data type
         * @param dependencies the required dependencies
         */
        public OperatorNodeInfo(
                ClassData classData,
                TypeDescription dataType,
                VertexElement... dependencies) {
            this(classData, dataType, Arrays.asList(Arguments.requireNonNull(dependencies)));
        }

        /**
         * Creates a new instance.
         * @param classData the generated class data
         * @param dataType the input data type
         * @param dependencies the required dependencies
         */
        public OperatorNodeInfo(
                ClassData classData,
                TypeDescription dataType,
                List<? extends VertexElement> dependencies) {
            Arguments.requireNonNull(classData);
            Arguments.requireNonNull(dataType);
            Arguments.requireNonNull(dependencies);
            this.classData = classData;
            this.dataType = dataType;
            this.dependencies = Arguments.freeze(dependencies);
        }

        @Override
        public ClassData getClassData() {
            return classData;
        }

        /**
         * Returns the data type.
         * @return the data type
         */
        @Override
        public TypeDescription getDataType() {
            return dataType;
        }

        @Override
        public List<VertexElement> getDependencies() {
            return dependencies;
        }

        @Override
        public String toString() {
            return String.format("Operator:%s(%s)->(%s)", //$NON-NLS-1$
                    getClassData(),
                    getDataType(),
                    getDependencies());
        }
    }

    /**
     * Represents the node info generated by {@link OperatorNodeGenerator}.
     * @since 0.4.0
     */
    class AggregateNodeInfo implements NodeInfo {

        private final ClassData classData;

        private final ClassDescription mapperType;

        private final ClassDescription copierType;

        private final ClassDescription combinerType;

        private final TypeDescription inputType;

        private final TypeDescription outputType;

        private final List<VertexElement> dependencies;

        /**
         * Creates a new instance.
         * @param classData the generated class data
         * @param mapperType the mapper type (nullable)
         * @param copierType the copier type (nullable)
         * @param combinerType the combiner type
         * @param inputType the input type
         * @param outputType the output type
         * @param dependencies the required dependencies
         */
        public AggregateNodeInfo(
                ClassData classData,
                ClassDescription mapperType,
                ClassDescription copierType,
                ClassDescription combinerType,
                TypeDescription inputType,
                TypeDescription outputType,
                List<? extends VertexElement> dependencies) {
            Arguments.requireNonNull(classData);
            Arguments.requireNonNull(combinerType);
            Arguments.requireNonNull(inputType);
            Arguments.requireNonNull(outputType);
            Arguments.requireNonNull(dependencies);
            this.classData = classData;
            this.mapperType = mapperType;
            this.copierType = copierType;
            this.combinerType = combinerType;
            this.inputType = inputType;
            this.outputType = outputType;
            this.dependencies = Arguments.freeze(dependencies);
        }

        @Override
        public ClassData getClassData() {
            return classData;
        }

        /**
         * Returns the mapper type.
         * @return the mapper type
         */
        public ClassDescription getMapperType() {
            return mapperType;
        }

        /**
         * Returns the combiner type.
         * @return the combiner type
         */
        public ClassDescription getCopierType() {
            return copierType;
        }

        /**
         * Returns the combiner type.
         * @return the combiner type
         */
        public ClassDescription getCombinerType() {
            return combinerType;
        }

        /**
         * Returns the input type.
         * @return the input type
         */
        public TypeDescription getInputType() {
            return inputType;
        }

        /**
         * Returns the output type.
         * @return the output type
         */
        public TypeDescription getOutputType() {
            return outputType;
        }

        @Override
        public TypeDescription getDataType() {
            return combinerType;
        }

        @Override
        public List<VertexElement> getDependencies() {
            return dependencies;
        }

        @Override
        public String toString() {
            return String.format("Aggregate:%s(%s)->(%s)", //$NON-NLS-1$
                    getClassData(),
                    getDataType(),
                    getDependencies());
        }
    }
}
