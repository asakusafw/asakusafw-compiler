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
package com.asakusafw.bridge.redirector;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.Type;

/**
 * Represents redirect rules.
 */
public class RedirectRule {

    private final Map<Type, Type> typeMapping = new HashMap<>();

    /**
     * Adds a mapping rule.
     * @param source binary name of the source type
     * @param destination binary name of the destination type
     */
    public void add(String source, String destination) {
        Type sourceType = Type.getObjectType(toInternalName(source));
        Type destinationType = Type.getObjectType(toInternalName(destination));
        add(sourceType, destinationType);
    }

    private String toInternalName(String binaryName) {
        return binaryName.replace('.', '/');
    }

    /**
     * Adds a mapping rule.
     * @param source the source type
     * @param destination the destination type
     */
    public void add(Type source, Type destination) {
        if (typeMapping.containsKey(source)) {
            if (destination.equals(typeMapping.get(source)) == false) {
                throw new IllegalStateException(MessageFormat.format(
                        "inconsistent rule for class: {0}",
                        source));
            }
        }
        this.typeMapping.put(source, destination);
    }

    /**
     * Returns the redirection target for the type.
     * @param type the source type
     * @return the destination type, or the source type if the source type is not a redirection target
     */
    public Type redirect(Type type) {
        Type destination = typeMapping.get(type);
        if (destination == null) {
            return type;
        }
        return destination;
    }
}
