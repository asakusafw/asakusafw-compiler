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
package com.asakusafw.dag.compiler.model.build;

import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents a resolved input.
 * @since 0.4.0
 */
public class ResolvedInputInfo {

    private final String id;

    private final String tag;

    private final ResolvedEdgeInfo edgeInfo;

    private final ClassDescription mapperType;

    private final ClassDescription copierType;

    private final ClassDescription combinerType;

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param edgeInfo the edge information
     */
    public ResolvedInputInfo(String id, ResolvedEdgeInfo edgeInfo) {
        this(id, null, edgeInfo, null, null, null);
    }

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param edgeInfo the edge information
     * @param mapperType the mapper type (nullable)
     * @param copierType the copier type (nullable)
     * @param combinerType the combiner type (nullable)
     */
    public ResolvedInputInfo(
            String id,
            ResolvedEdgeInfo edgeInfo,
            ClassDescription mapperType,
            ClassDescription copierType,
            ClassDescription combinerType) {
        this(id, null, edgeInfo, mapperType, copierType, combinerType);
    }

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param tag the optional port tag (nullable)
     * @param edgeInfo the edge information
     */
    public ResolvedInputInfo(String id, String tag, ResolvedEdgeInfo edgeInfo) {
        this(id, tag, edgeInfo, null, null, null);
    }

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param tag the optional port tag
     * @param edgeInfo the edge information
     * @param mapperType the mapper type (nullable)
     * @param copierType the copier type (nullable)
     * @param combinerType the combiner type (nullable)
     */
    public ResolvedInputInfo(
            String id, String tag,
            ResolvedEdgeInfo edgeInfo,
            ClassDescription mapperType,
            ClassDescription copierType,
            ClassDescription combinerType) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(edgeInfo);
        this.id = id;
        this.tag = tag;
        this.edgeInfo = edgeInfo;
        this.mapperType = mapperType;
        this.copierType = copierType;
        this.combinerType = combinerType;
    }

    /**
     * Returns the ID.
     * @return the ID
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the tag.
     * @return the tag, or {@code null} if it is not defined
     */
    public String getTag() {
        return tag;
    }

    /**
     * Returns the edge information.
     * @return the edge information
     */
    public ResolvedEdgeInfo getEdgeInfo() {
        return edgeInfo;
    }

    /**
     * Returns the descriptor.
     * @return the descriptor
     */
    public EdgeDescriptor getDescriptor() {
        return edgeInfo.getDescriptor();
    }

    /**
     * Returns the mapper type.
     * @return the mapper type, or {@code null} if it is not defined
     */
    public ClassDescription getMapperType() {
        return mapperType;
    }

    /**
     * Returns the copier type.
     * @return the copier type, or {@code null} if it is not defined
     */
    public ClassDescription getCopierType() {
        return copierType;
    }

    /**
     * Returns the combiner type.
     * @return the combiner type, or {@code null} if it is not defined
     */
    public ClassDescription getCombinerType() {
        return combinerType;
    }
}
