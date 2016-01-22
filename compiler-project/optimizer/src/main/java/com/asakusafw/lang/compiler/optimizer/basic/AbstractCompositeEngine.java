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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.CustomOperator;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.optimizer.EngineBinding;

/**
 * Composition of engines.
 * @param <T> the engine type
 * @since 0.1.0
 * @version 0.3.0
 */
public abstract class AbstractCompositeEngine<T> {

    private final T defaultElement;

    private final Map<OperatorKind, ? extends T> kinds;

    private final Map<String, ? extends T> inputs;

    private final Map<String, ? extends T> outputs;

    private final Map<CoreOperatorKind, ? extends T> cores;

    private final Map<ClassDescription, ? extends T> users;

    private final Map<String, ? extends T> customs;

    AbstractCompositeEngine(
            T defaultElement,
            Map<OperatorKind, ? extends T> kinds,
            Map<String, ? extends T> inputs,
            Map<String, ? extends T> outputs,
            Map<CoreOperatorKind, ? extends T> cores,
            Map<ClassDescription, ? extends T> users,
            Map<String, ? extends T> customs) {
        this.defaultElement = defaultElement;
        this.kinds = kinds;
        this.inputs = inputs;
        this.outputs = outputs;
        this.cores = cores;
        this.users = users;
        this.customs = customs;
    }

    /**
     * Returns the element for the target operator.
     * @param operator the target operator
     * @return the element, or {@code null} if it is not found
     */
    protected T getElement(Operator operator) {
        switch (operator.getOperatorKind()) {
        case CORE:
            return findFromCores((CoreOperator) operator);
        case USER:
            return findFromUsers((UserOperator) operator);
        case CUSTOM:
            return findFromCustoms((CustomOperator) operator);
        case INPUT:
            return findFromInputs((ExternalInput) operator);
        case OUTPUT:
            return findFromOutputs((ExternalOutput) operator);
        default:
            return findFromDefaults(operator);
        }
    }

    private T findFromCores(CoreOperator operator) {
        T found = cores.get(operator.getCoreOperatorKind());
        if (found != null) {
            return found;
        }
        return findFromDefaults(operator);
    }

    private T findFromUsers(UserOperator operator) {
        T found = users.get(operator.getAnnotation().getDeclaringClass());
        if (found != null) {
            return found;
        }
        return findFromDefaults(operator);
    }

    private T findFromCustoms(CustomOperator operator) {
        T found = customs.get(operator.getCategory());
        if (found != null) {
            return found;
        }
        return findFromDefaults(operator);
    }

    private T findFromInputs(ExternalInput operator) {
        if (operator.isExternal()) {
            T found = inputs.get(operator.getInfo().getModuleName());
            if (found != null) {
                return found;
            }
        }
        return findFromDefaults(operator);
    }

    private T findFromOutputs(ExternalOutput operator) {
        if (operator.isExternal()) {
            T found = outputs.get(operator.getInfo().getModuleName());
            if (found != null) {
                return found;
            }
        }
        return findFromDefaults(operator);
    }

    private T findFromDefaults(Operator operator) {
        T found = kinds.get(operator.getOperatorKind());
        if (found != null) {
            return found;
        }
        return defaultElement;
    }

    /**
     * Builder for {@link AbstractCompositeEngine}.
     * @param <TSelf> the implementation type
     * @param <TElement> the engine type
     */
    public abstract static class AbstractBuilder<TSelf extends AbstractBuilder<TSelf, TElement>, TElement> {

        static final Map<ClassDescription, CoreOperatorKind> CORE_ANNOTATION_TYPES;
        static {
            Map<ClassDescription, CoreOperatorKind> map = new HashMap<>();
            for (CoreOperatorKind kind : CoreOperatorKind.values()) {
                map.put(kind.getAnnotationType(), kind);
            }
            CORE_ANNOTATION_TYPES = map;
        }

        private TElement root;

        private final Map<OperatorKind, TElement> defaults = new HashMap<>();

        private final Map<String, TElement> inputs = new HashMap<>();

        private final Map<String, TElement> outputs = new HashMap<>();

        private final Map<CoreOperatorKind, TElement> cores = new HashMap<>();

        private final Map<ClassDescription, TElement> users = new HashMap<>();

        private final Map<String, TElement> customs = new HashMap<>();

        /**
         * Returns the implementation type.
         * @return this
         */
        @SuppressWarnings("unchecked")
        protected TSelf getSelf() {
            return (TSelf) this;
        }

        /**
         * Creates a new instance.
         * @param defaultElement element for default
         * @param kindElements elements for each operator kind
         * @param inputElements elements for input operators
         * @param outputElements elements for output operators
         * @param coreElements elements for core operators
         * @param userElements elements for user operators
         * @param customElements elements for custom operators
         * @return the built instance
         */
        protected abstract TElement doBuild(
                TElement defaultElement,
                Map<OperatorKind, TElement> kindElements,
                Map<String, TElement> inputElements,
                Map<String, TElement> outputElements,
                Map<CoreOperatorKind, TElement> coreElements,
                Map<ClassDescription, TElement> userElements,
                Map<String, TElement> customElements);

        /**
         * Sets elements from via SPI.
         * @param loader the service loader
         * @param bindingType the binding type
         * @return this
         */
        public TSelf load(ClassLoader loader, Class<? extends EngineBinding<? extends TElement>> bindingType) {
            for (EngineBinding<? extends TElement> binding : Util.load(loader, bindingType)) {
                withBinding(binding);
            }
            return getSelf();
        }

        /**
         * Sets the operator element using binding.
         * @param binding the binding information
         * @return this
         */
        public TSelf withBinding(EngineBinding<? extends TElement> binding) {
            TElement element = binding.getEngine();
            assert element != null;
            for (ClassDescription type : binding.getTargetOperators()) {
                withUser(type, element);
            }
            for (String category : binding.getTargetCategories()) {
                withCustom(category, element);
            }
            for (String name : binding.getTargetInputs()) {
                withInput(name, element);
            }
            for (String name : binding.getTargetOutputs()) {
                withOutput(name, element);
            }
            return getSelf();
        }

        /**
         * Sets the default operator element.
         * @param element the element
         * @return this
         */
        public TSelf withDefault(TElement element) {
            this.root = element;
            return getSelf();
        }

        /**
         * Sets the default input operator element.
         * @param element the element
         * @return this
         */
        public TSelf withInput(TElement element) {
            return withDefault(OperatorKind.INPUT, element);
        }

        /**
         * Sets the operator element for the target input.
         * @param moduleName the external input module (nullable)
         * @param element the element
         * @return this
         * @see ExternalInputInfo#getModuleName()
         */
        public TSelf withInput(String moduleName, TElement element) {
            if (moduleName == null) {
                withInput(element);
            } else {
                inputs.put(moduleName, element);
            }
            return getSelf();
        }

        /**
         * Sets the default output operator element.
         * @param element the element
         * @return this
         */
        public TSelf withOutput(TElement element) {
            return withDefault(OperatorKind.OUTPUT, element);
        }

        /**
         * Sets the operator element for the target output.
         * @param moduleName the external output module (nullable)
         * @param element the element
         * @return this
         * @see ExternalOutputInfo#getModuleName()
         */
        public TSelf withOutput(String moduleName, TElement element) {
            if (moduleName == null) {
                withOutput(element);
            } else {
                outputs.put(moduleName, element);
            }
            return getSelf();
        }

        /**
         * Sets the operator element for the target core operator.
         * @param kind the target core operator kind
         * @param element the element
         * @return this
         * @see CoreOperator#getCoreOperatorKind()
         */
        public TSelf withCore(CoreOperatorKind kind, TElement element) {
            cores.put(kind, element);
            return getSelf();
        }

        /**
         * Sets the operator element for the target user operator.
         * @param annotationType the target operator annotation type
         * @param element the element
         * @return this
         * @see UserOperator#getAnnotation()
         */
        public TSelf withUser(ClassDescription annotationType, TElement element) {
            if (annotationType == null) {
                withDefault(OperatorKind.CORE, element);
                withDefault(OperatorKind.USER, element);
            } else {
                CoreOperatorKind coreKind = CORE_ANNOTATION_TYPES.get(annotationType);
                if (coreKind != null) {
                    cores.put(coreKind, element);
                } else {
                    users.put(annotationType, element);
                }
            }
            return getSelf();
        }

        /**
         * Sets the operator element for the target user operator.
         * @param annotationType the target operator annotation type
         * @param element the element
         * @return this
         * @see UserOperator#getAnnotation()
         */
        public TSelf withUser(Class<? extends Annotation> annotationType, TElement element) {
            return withUser(ClassDescription.of(annotationType), element);
        }

        /**
         * Sets the operator element for the target user operator.
         * @param annotationTypes the target operator annotation types
         * @param element the element
         * @return this
         * @see UserOperator#getAnnotation()
         */
        public TSelf withUser(Collection<? extends ClassDescription> annotationTypes, TElement element) {
            for (ClassDescription annotationType : annotationTypes) {
                withUser(annotationType, element);
            }
            return getSelf();
        }

        /**
         * Sets the operator element for the target custom operator.
         * @param category the custom category tag
         * @param element the element
         * @return this
         * @see CustomOperator#getCategory()
         * @since 0.3.0
         */
        public TSelf withCustom(String category, TElement element) {
            customs.put(category, element);
            return getSelf();
        }

        /**
         * Sets the default marker operator element.
         * @param element the element
         * @return this
         */
        public TSelf withMarker(TElement element) {
            return withDefault(OperatorKind.MARKER, element);
        }

        /**
         * Builds instance.
         * @return the built instance
         */
        public TElement build() {
            return doBuild(root, defaults, inputs, outputs, cores, users, customs);
        }

        private TSelf withDefault(OperatorKind kind, TElement element) {
            defaults.put(kind, element);
            return getSelf();
        }
    }
}
