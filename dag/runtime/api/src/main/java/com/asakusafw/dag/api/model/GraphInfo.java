/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.lang.utils.common.Arguments;

/**
 * Describes Asakusa DAG root.
 * @since 0.4.0
 */
public class GraphInfo implements Serializable {

    private static final byte EOF = 0x1a;

    private static final int MAGIC = 0xa343ada0;

    private static final int VERSION = 2;

    private static final long serialVersionUID = VERSION;

    private final List<VertexInfo> vertices = new ArrayList<>();

    private final List<EdgeInfo> edges = new ArrayList<>();

    /**
     * Returns the vertices.
     * @return the vertices
     */
    public List<VertexInfo> getVertices() {
        return vertices;
    }

    /**
     * Returns the edges.
     * @return the edges
     */
    public List<EdgeInfo> getEdges() {
        return edges;
    }

    /**
     * Adds a vertex.
     * @param name the vertex name
     * @param descriptor the processor descriptor
     * @return the added vertex
     */
    public VertexInfo addVertex(String name, VertexDescriptor descriptor) {
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(descriptor);
        if (vertices.stream().anyMatch(v -> v.getName().equals(name))) {
            throw new IllegalStateException(MessageFormat.format(
                    "duplicate vertex: {0}",
                    name));
        }
        VertexInfo info = new VertexInfo(new VertexId(name), descriptor);
        vertices.add(info);
        return info;
    }

    /**
     * Adds an edge.
     * @param upstream the upstream port ID
     * @param downstream the downstream port ID
     * @param descriptor the processor descriptor
     * @return the added edge
     */
    public EdgeInfo addEdge(PortId upstream, PortId downstream, EdgeDescriptor descriptor) {
        Arguments.requireNonNull(upstream);
        Arguments.requireNonNull(downstream);
        Arguments.requireNonNull(descriptor);
        EdgeInfo info = new EdgeInfo(upstream, downstream, descriptor);
        edges.add(info);
        return info;
    }

    /**
     * Saves a {@link GraphInfo} object into the stream.
     * @param output the target output stream
     * @param info the graph to save
     * @throws IOException if error was occurred while saving the graph
     */
    public static void save(OutputStream output, GraphInfo info) throws IOException {
        Arguments.requireNonNull(output);
        Arguments.requireNonNull(info);
        ObjectOutputStream out = new ObjectOutputStream(output);
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeObject(info);
        out.reset();
        out.writeByte(EOF);
        out.flush();
    }

    /**
     * Loads a {@link GraphInfo} object from the stream.
     * @param input the source input stream
     * @return the restored graph
     * @throws IOException if error was occurred while restoring the graph
     */
    public static GraphInfo load(InputStream input) throws IOException {
        Arguments.requireNonNull(input);
        ObjectInputStream in = new ObjectInputStream(input);
        int first = in.readInt();
        if (first != MAGIC) {
            throw new IOException("broken GraphInfo file (invalid MAGIC)"); //$NON-NLS-1$
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("unsupported GraphInfo format (invalid VERSION)"); //$NON-NLS-1$
        }
        Object object;
        try {
            object = in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("error occurred while restoring GraphInfo", e);
        }
        byte last = in.readByte();
        if (last != EOF) {
            throw new IOException("GraphInfo file broken (missing EOF)");
        }
        if (object instanceof GraphInfo) {
            return (GraphInfo) object;
        } else {
            throw new IOException("broken GraphInfo file (invalid object type)");
        }
    }
}
