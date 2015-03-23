/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.operator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.lang.model.type.TypeMirror;

import com.asakusafw.lang.compiler.model.description.EnumConstantDescription;

/**
 * Represents an operator's semantics for Asakusa DSL.
 */
public class OperatorDescription {

    private final Document document;

    private final List<Node> parameters;

    private final List<Node> outputs;

    private final List<EnumConstantDescription> attributes;

    /**
     * Creates a new instance.
     * @param document the document about the operator
     * @param parameters parameters (input datasets/arguments)
     * @param outputs output datasets
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public OperatorDescription(
            Document document,
            List<? extends Node> parameters,
            List<? extends Node> outputs) {
        this(document, parameters, outputs, Collections.<EnumConstantDescription>emptyList());
    }

    /**
     * Creates a new instance.
     * @param document the document about the operator
     * @param parameters parameters (input datasets/arguments)
     * @param outputs output datasets
     * @param attributes operator attributes
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public OperatorDescription(
            Document document,
            List<? extends Node> parameters,
            List<? extends Node> outputs,
            List<? extends EnumConstantDescription> attributes) {
        if (document == null) {
            throw new IllegalArgumentException("document must not be null"); //$NON-NLS-1$
        }
        if (parameters == null) {
            throw new IllegalArgumentException("parameters must not be null"); //$NON-NLS-1$
        }
        if (outputs == null) {
            throw new IllegalArgumentException("outputs must not be null"); //$NON-NLS-1$
        }
        if (attributes == null) {
            throw new IllegalArgumentException("attributes must not be null"); //$NON-NLS-1$
        }
        this.document = document;
        this.parameters = new ArrayList<>(parameters);
        this.outputs = new ArrayList<>(outputs);
        this.attributes = new ArrayList<>(attributes);
    }

    /**
     * Returns the documents about the operator.
     * @return the document
     */
    public Document getDocument() {
        return document;
    }

    /**
     * Returns the parameters, which include input datasets and arguments.
     * @return the parameters
     */
    public List<Node> getParameters() {
        return parameters;
    }

    /**
     * Returns the input datasets.
     * @return the inputs
     */
    public List<Node> getInputs() {
        List<Node> results = new ArrayList<>();
        for (Node node : parameters) {
            if (node.getKind() == Node.Kind.INPUT) {
                results.add(node);
            }
        }
        return results;
    }

    /**
     * Returns the input arguments.
     * @return the arguments
     */
    public List<Node> getArguments() {
        List<Node> results = new ArrayList<>();
        for (Node node : parameters) {
            if (node.getKind() == Node.Kind.DATA) {
                results.add(node);
            }
        }
        return results;
    }

    /**
     * Returns the output datasets.
     * @return the outputs
     */
    public List<Node> getOutputs() {
        return outputs;
    }

    /**
     * Returns input/output/arguments.
     * @return all node elements
     */
    public List<Node> getAllNodes() {
        List<Node> results = new ArrayList<>();
        results.addAll(parameters);
        results.addAll(outputs);
        return results;
    }

    /**
     * Returns the operator attributes.
     * @return the operator attributes
     */
    public List<EnumConstantDescription> getAttributes() {
        return attributes;
    }

    /**
     * Represents reference to original declaration.
     */
    public abstract static class Reference {

        /**
         * Creates a new instance.
         * @return the created instance.
         */
        public static Reference method() {
            return new MethodReference();
        }

        /**
         * Creates a new instance.
         * @return the created instance.
         */
        public static Reference returns() {
            return new ReturnReference();
        }

        /**
         * Creates a new instance.
         * @param location the parameter index (0-origin)
         * @return the created instance.
         */
        public static ParameterReference parameter(int location) {
            return new ParameterReference(location);
        }

        /**
         * Creates a new instance.
         * @param info operator specific information
         * @return the created instance.
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public static SpecialReference special(String info) {
            return new SpecialReference(info);
        }

        /**
         * Returns the kind of this reference.
         * @return the kind
         */
        public abstract Kind getKind();

        @Override
        public int hashCode() {
            return getKind().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }

        /**
         * Represents kind of references.
         */
        public enum Kind {

            /**
             * Represents method declaration, or synopsis block in documentation comment.
             */
            METHOD,

            /**
             * Represents a method parameter, or parameter block in documentation comment.
             */
            PARAMETER,

            /**
             * Represents a method return type, or return block in documentation comment.
             */
            RETURN,

            /**
             * Represents a operator specific reference.
             */
            SPECIAL,
        }
    }

    /**
     * Represents a reference to body of declaration.
     */
    public static final class MethodReference extends Reference {

        @Override
        public Kind getKind() {
            return Kind.METHOD;
        }
    }

    /**
     * Represents a reference to a parameter declaration.
     */
    public static final class ParameterReference extends Reference {

        private final int location;

        /**
         * Creates a new instance.
         * @param location the parameter location (0-origin)
         */
        public ParameterReference(int location) {
            this.location = location;
        }

        @Override
        public Kind getKind() {
            return Kind.PARAMETER;
        }

        /**
         * Returns the location of parameter (0-origin).
         * @return the location
         */
        public int getLocation() {
            return location;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = getKind().hashCode();
            result = prime * result + location;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ParameterReference other = (ParameterReference) obj;
            if (location != other.location) {
                return false;
            }
            return true;
        }
    }

    /**
     * Represents a reference to return type/value of declaration.
     */
    public static final class ReturnReference extends Reference {

        @Override
        public Kind getKind() {
            return Kind.RETURN;
        }
    }

    /**
     * Represents an operator specific reference.
     */
    public static final class SpecialReference extends Reference {

        private final String info;

        /**
         * Creates a new instance.
         * @param info reference info
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public SpecialReference(String info) {
            if (info == null) {
                throw new IllegalArgumentException("info must not be null"); //$NON-NLS-1$
            }
            this.info = info;
        }

        @Override
        public Kind getKind() {
            return Kind.PARAMETER;
        }

        /**
         * Return the operator specific information.
         * @return the the operator specific information
         */
        public String getInfo() {
            return info;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + info.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SpecialReference other = (SpecialReference) obj;
            if (!info.equals(other.info)) {
                return false;
            }
            return true;
        }
    }

    /**
     * Represents document.
     */
    public abstract static class Document {

        /**
         * Creates a new instance.
         * @param text text of this document
         * @return the created instance
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public static TextDocument text(String text) {
            return new TextDocument(text);
        }

        /**
         * Creates a new instance.
         * @param reference reference description
         * @return the created instance
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public static ReferenceDocument reference(Reference reference) {
            return new ReferenceDocument(reference);
        }

        /**
         * Returns the kind of this document.
         * @return the kind
         */
        public abstract Kind getKind();

        /**
         * Represents document kind.
         */
        public enum Kind {

            /**
             * Just text.
             */
            TEXT,

            /**
             * Reference to other document.
             */
            REFERENCE,
        }
    }

    /**
     * Represents a document with text.
     */
    public static class TextDocument extends Document {

        private final String text;

        /**
         * Creates a new instance.
         * @param text text of this document
         */
        public TextDocument(String text) {
            this.text = text;
        }

        /**
         * Return the text of this document.
         * @return the text, or {@code null} if not declared
         */
        public String getText() {
            return text;
        }

        @Override
        public Kind getKind() {
            return Kind.TEXT;
        }
    }

    /**
     * Represents a document with reference.
     */
    public static class ReferenceDocument extends Document {

        private final Reference reference;

        /**
         * Creates a new instance.
         * @param reference reference description
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public ReferenceDocument(Reference reference) {
            if (reference == null) {
                throw new IllegalArgumentException("reference must not be null"); //$NON-NLS-1$
            }
            this.reference = reference;
        }

        @Override
        public Kind getKind() {
            return Kind.REFERENCE;
        }

        /**
         * Returns the reference which represents the document location in original method declaration.
         * @return the reference
         */
        public Reference getReference() {
            return reference;
        }
    }

    /**
     * Represents input/output/argument.
     */
    public static final class Node {

        private final Kind kind;

        private final String name;

        private final Document document;

        private final TypeMirror type;

        private final Reference reference;

        private volatile KeyMirror key;

        private volatile ExternMirror extern;

        /**
         * Creates a new instance.
         * @param kind the kind of this node
         * @param name the name of this node
         * @param document the document for this node
         * @param type the type of this node
         * @param reference the reference to original declaration
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public Node(Kind kind, String name, Document document, TypeMirror type, Reference reference) {
            if (kind == null) {
                throw new IllegalArgumentException("kind must not be null"); //$NON-NLS-1$
            }
            if (name == null) {
                throw new IllegalArgumentException("name must not be null"); //$NON-NLS-1$
            }
            if (document == null) {
                throw new IllegalArgumentException("document must not be null"); //$NON-NLS-1$
            }
            if (type == null) {
                throw new IllegalArgumentException("type must not be null"); //$NON-NLS-1$
            }
            if (reference == null) {
                throw new IllegalArgumentException("reference must not be null"); //$NON-NLS-1$
            }
            this.kind = kind;
            this.name = name;
            this.document = document;
            this.type = type;
            this.reference = reference;
        }

        /**
         * Returns the kind of this node.
         * @return the kind
         */
        public Kind getKind() {
            return kind;
        }

        /**
         * Returns the name of this node.
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the document for this node.
         * @return the document
         */
        public Document getDocument() {
            return document;
        }

        /**
         * Returns the type of this node.
         * @return the type
         */
        public TypeMirror getType() {
            return type;
        }

        /**
         * Returns the reference to original component.
         * @return the reference to original component
         */
        public Reference getReference() {
            return reference;
        }

        /**
         * Sets the key mirror.
         * @param newValue the key mirror
         * @return this
         */
        public Node withKey(KeyMirror newValue) {
            this.key = newValue;
            return this;
        }

        /**
         * Returns the key mirror of this node.
         * @return the key mirror, or {@code null} if this does not have any keys
         */
        public KeyMirror getKey() {
            return key;
        }

        /**
         * Sets the extern mirror.
         * @param newValue the extern mirror
         * @return this
         */
        public Node withExtern(ExternMirror newValue) {
            this.extern = newValue;
            return this;
        }

        /**
         * Returns the extern mirror of this node.
         * @return the extern mirror, or {@code null} if this does not have any externs
         */
        public ExternMirror getExtern() {
            return extern;
        }

        /**
         * Represents parameter kind.
         */
        public enum Kind {

            /**
             * dataset input.
             */
            INPUT,

            /**
             * dataset output.
             */
            OUTPUT,

            /**
             * Constant expression.
             */
            DATA,
        }
    }
}
