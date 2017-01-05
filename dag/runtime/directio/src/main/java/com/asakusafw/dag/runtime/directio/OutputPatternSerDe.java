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
package com.asakusafw.dag.runtime.directio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DateUtil;

/**
 * Ser/De for Direct I/O file output patterns.
 * @since 0.4.0
 */
public abstract class OutputPatternSerDe implements KeyValueSerDe {

    private final List<Fragment> fragments = new ArrayList<>();

    private final List<Variable> variables = new ArrayList<>();

    private final List<RandomNumber> randoms = new ArrayList<>();

    private final StringBuilder keyBuffer = new StringBuilder();

    /**
     * Appends a constant.
     * @param value the constant value
     * @return this
     */
    public final OutputPatternSerDe text(String value) {
        Arguments.requireNonNull(value);
        return append(new Constant(value));
    }

    /**
     * Appends a variable.
     * @param format the format type
     * @param argument the format argument (nullable)
     * @return this
     */
    public final OutputPatternSerDe property(Format format, String argument) {
        Arguments.requireNonNull(format);
        return append(format.newFragment(argument));
    }

    /**
     * Appends a random number.
     * @param min the min value
     * @param max the max value
     * @return this
     */
    public final OutputPatternSerDe random(int min, int max) {
        Arguments.require(min <= max);
        long seed = 0xcafebabe + randoms.size() * 31;
        return append(new RandomNumber(seed, min, max));
    }

    private OutputPatternSerDe append(Fragment fragment) {
        if (fragment instanceof Variable) {
            this.variables.add((Variable) fragment);
        } else if (fragment instanceof RandomNumber) {
            this.randoms.add((RandomNumber) fragment);
        }
        fragments.add(fragment);
        return this;
    }

    /**
     * Returns the property value.
     * @param object the target container
     * @param index the property index in the target container
     * @return the related property value
     */
    protected Object getProperty(Object object, int index) {
        throw new NoSuchElementException();
    }

    @Override
    public void serializeKey(Object object, DataOutput output) throws IOException, InterruptedException {
        List<Variable> vs = variables;
        for (int i = 0, n = vs.size(); i < n; i++) {
            vs.get(i).update(getProperty(object, i));
        }
        List<RandomNumber> rs = randoms;
        for (int i = 0, n = rs.size(); i < n; i++) {
            rs.get(i).update();
        }
        List<Fragment> fs = fragments;
        for (int i = 0, n = fs.size(); i < n; i++) {
            fs.get(i).write(output);
        }
    }

    @Override
    public Object deserializeKey(DataInput keyInput) throws IOException, InterruptedException {
        StringBuilder buf = keyBuffer;
        buf.setLength(0);
        List<Fragment> fs = fragments;
        for (int i = 0, n = fs.size(); i < n; i++) {
            Fragment f = fs.get(i);
            f.readFields(keyInput);
            f.appendTo(buf);
        }
        return buf.toString();
    }

    /**
     * Format of each fragment.
     */
    public enum Format {

        /**
         * Converts naturally (format string will be ignored).
         */
        NATURAL {
            @Override
            public Fragment newFragment(String formatString) {
                return new Variable() {
                    @Override
                    void update(Object propertyValue) {
                        value = (String.valueOf(propertyValue));
                    }
                };
            }
        },

        /**
         * Converts {@link Date} date (use {@link SimpleDateFormat}).
         */
        DATE {
            @Override
            public Fragment newFragment(String formatString) {
                Arguments.requireNonNull(formatString);
                Calendar calendar = Calendar.getInstance();
                DateFormat dateFormat = new SimpleDateFormat(formatString);
                return new Variable() {
                    @Override
                    void update(Object propertyValue) {
                        DateOption option = (DateOption) propertyValue;
                        if (option.isNull()) {
                            value = String.valueOf(option);
                        } else {
                            Date date = option.get();
                            DateUtil.setDayToCalendar(date.getElapsedDays(), calendar);
                            value = String.valueOf(dateFormat.format(calendar.getTime()));
                        }
                    }
                };
            }
        },

        /**
         * Converts {@link DateTime} date (use {@link SimpleDateFormat}).
         */
        DATETIME {
            @Override
            public Fragment newFragment(String formatString) {
                Arguments.requireNonNull(formatString);
                Calendar calendar = Calendar.getInstance();
                DateFormat dateFormat = new SimpleDateFormat(formatString);
                return new Variable() {
                    @Override
                    void update(Object propertyValue) {
                        DateTimeOption option = (DateTimeOption) propertyValue;
                        if (option.isNull()) {
                            value = String.valueOf(option);
                        } else {
                            DateTime date = option.get();
                            DateUtil.setSecondToCalendar(date.getElapsedSeconds(), calendar);
                            value = String.valueOf(dateFormat.format(calendar.getTime()));
                        }
                    }
                };
            }
        },
        ;

        /**
         * Creates a new formatter using the format string.
         * @param formatString format string
         * @return the created formatter
         */
        abstract Fragment newFragment(String formatString);
    }

    private interface Fragment extends Writable {

        void appendTo(StringBuilder target);
    }

    private static final class Constant implements Fragment {

        private final String value;

        Constant(String value) {
            this.value = value;
        }

        @Override
        public void appendTo(StringBuilder target) {
            target.append(value);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(0);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            in.readByte();
        }
    }

    private abstract static class Variable implements Fragment {

        String value = ""; //$NON-NLS-1$

        Variable() {
            return;
        }

        abstract void update(Object propertyValue);

        @Override
        public void appendTo(StringBuilder target) {
            target.append(value);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value = in.readUTF();
        }
    }

    private static final class RandomNumber implements Fragment {

        private final Random random;

        private final int min;

        private final int max;

        private int current;

        RandomNumber(long seed, int min, int max) {
            this.random = new Random(seed);
            this.min = min;
            this.max = max;
            this.current = min;
        }

        public void update() {
            current = random.nextInt(max - min + 1) + min;
        }

        @Override
        public void appendTo(StringBuilder target) {
            target.append(current);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(current);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            current = in.readInt();
        }
    }
}
