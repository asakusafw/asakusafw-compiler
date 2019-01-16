/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.bridge.hadoop.combine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Represents a combined {@link InputSplit}.
 * @since 0.4.2
 */
public class CombinedInputSplit extends InputSplit implements Writable, Configurable {

    private Configuration conf;

    private Class<? extends InputSplit> splitType;

    private List<InputSplit> splits;

    private List<String> locations;

    /**
     * Creates a new instance for the serialization framework.
     */
    public CombinedInputSplit() {
        return;
    }

    /**
     * Creates a new instance.
     * @param conf the configuration
     * @param splits the combined splits
     * @param locations the combined locations
     */
    public CombinedInputSplit(Configuration conf, List<? extends InputSplit> splits, List<String> locations) {
        if (splits.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.conf = conf;
        this.splitType = splits.get(0).getClass();
        this.splits = new ArrayList<>(splits);
        this.locations = new ArrayList<>(locations);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Returns the combined splits.
     * @return the combined splits
     */
    public List<InputSplit> getSplits() {
        return splits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        long total = 0;
        for (InputSplit split : splits) {
            total += split.getLength();
        }
        return total;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, splitType.getName());
        WritableUtils.writeVInt(out, splits.size());
        for (InputSplit s : splits) {
            ((Writable) s).write(out);
        }
        WritableUtils.writeVInt(out, locations.size());
        for (String s : locations) {
            Text.writeString(out, s);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        if (conf == null) {
            throw new IllegalStateException();
        }
        try {
            splitType = (Class<? extends InputSplit>) conf.getClassByName(Text.readString(in));
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        this.splits = new ArrayList<>();
        int nSplits = WritableUtils.readVInt(in);
        for (int i = 0; i < nSplits; i++) {
            InputSplit s = ReflectionUtils.newInstance(splitType, conf);
            ((Writable) s).readFields(in);
            splits.add(s);
        }
        this.locations = new ArrayList<>();
        int nLocations = WritableUtils.readVInt(in);
        for (int i = 0; i < nLocations; i++) {
            String s = Text.readString(in);
            locations.add(s);
        }
    }
}
