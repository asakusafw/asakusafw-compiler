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
package com.asakusafw.bridge.hadoop.directio;

import static com.asakusafw.bridge.hadoop.directio.Util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.DirectInputFragment;

/**
 * An {@link InputSplit} for {@link DirectFileInputFormat}.
 */
public class DirectFileInputSplit extends InputSplit implements Configurable, Writable {

    private Configuration conf;

    private String containerPath;

    private DataDefinition<?> definition;

    private DirectInputFragment fragment;

    /**
     * Creates a new instance for serializers.
     */
    public DirectFileInputSplit() {
        return;
    }

    /**
     * Creates a new instance.
     * @param containerPath the container path
     * @param definition the data definition
     * @param fragment the target input fragment
     */
    public DirectFileInputSplit(String containerPath, DataDefinition<?> definition, DirectInputFragment fragment) {
        this.containerPath = containerPath;
        this.definition = definition;
        this.fragment = fragment;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, containerPath);
        writeDataDefinition(out, definition);
        writeFragment(out, fragment);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        containerPath = Text.readString(in);
        definition = readDataDefinition(in, conf);
        fragment = readFragment(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return fragment.getSize();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        List<String> locations = fragment.getOwnerNodeNames();
        return locations.toArray(new String[locations.size()]);
    }

    /**
     * Returns the path for detecting {@link DirectDataSource}.
     * @return the container path
     * @see DirectDataSourceRepository#getRelatedDataSource(String)
     */
    public String getContainerPath() {
        return containerPath;
    }

    /**
     * Returns the data definition.
     * @return the data definition
     */
    public DataDefinition<?> getDataDefinition() {
        return definition;
    }

    /**
     * Returns the target input fragment.
     * @return the target input fragment
     */
    public DirectInputFragment getInputFragment() {
        return fragment;
    }
}