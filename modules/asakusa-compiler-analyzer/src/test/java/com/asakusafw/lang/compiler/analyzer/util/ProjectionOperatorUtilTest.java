package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link ProjectionOperatorUtil}.
 */
public class ProjectionOperatorUtilTest {

    private final DataModelLoader dml = new MockDataModelLoader(getClass().getClassLoader());

    /**
     * supported types.
     */
    @Test
    public void support() {
        CoreOperator m0 = core(CoreOperatorKind.PROJECT, Ksv.class, Kv.class);
        CoreOperator m1 = core(CoreOperatorKind.EXTEND, Ksv.class, Kv.class);
        CoreOperator m2 = core(CoreOperatorKind.RESTRUCTURE, Ksv.class, Kv.class);
        CoreOperator m3 = core(CoreOperatorKind.CHECKPOINT, Kv.class, Kv.class);
        UserOperator m4 = user();

        assertThat(ProjectionOperatorUtil.isSupported(m0), is(true));
        assertThat(ProjectionOperatorUtil.isSupported(m1), is(true));
        assertThat(ProjectionOperatorUtil.isSupported(m2), is(true));
        assertThat(ProjectionOperatorUtil.isSupported(m3), is(false));
        assertThat(ProjectionOperatorUtil.isSupported(m4), is(false));
    }

    /**
     * project operator.
     */
    @Test
    public void project_narrowing() {
        CoreOperator operator = core(CoreOperatorKind.PROJECT, Ksv.class, Kv.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * project operator.
     */
    @Test(expected = DiagnosticException.class)
    public void project_widening() {
        CoreOperator operator = core(CoreOperatorKind.PROJECT, Kv.class, Ksv.class);
        ProjectionOperatorUtil.getPropertyMappings(dml, operator);
    }

    /**
     * project operator.
     */
    @Test
    public void project_equivalent() {
        CoreOperator operator = core(CoreOperatorKind.PROJECT, Kv.class, KvEquivalent.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * extend operator.
     */
    @Test(expected = DiagnosticException.class)
    public void extend_narrowing() {
        CoreOperator operator = core(CoreOperatorKind.EXTEND, Ksv.class, Kv.class);
        ProjectionOperatorUtil.getPropertyMappings(dml, operator);
    }

    /**
     * extend operator.
     */
    @Test
    public void extend_widening() {
        CoreOperator operator = core(CoreOperatorKind.EXTEND, Kv.class, Ksv.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * extend operator.
     */
    @Test
    public void extend_equivalent() {
        CoreOperator operator = core(CoreOperatorKind.EXTEND, Kv.class, KvEquivalent.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * restructure operator.
     */
    @Test
    public void restructure_narrowing() {
        CoreOperator operator = core(CoreOperatorKind.RESTRUCTURE, Ksv.class, Kv.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * restructure operator.
     */
    @Test
    public void restructure_widening() {
        CoreOperator operator = core(CoreOperatorKind.RESTRUCTURE, Kv.class, Ksv.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * restructure operator.
     */
    @Test
    public void restructure_equivalent() {
        CoreOperator operator = core(CoreOperatorKind.RESTRUCTURE, Kv.class, KvEquivalent.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * restructure operator.
     */
    @Test
    public void restructure_orthogonal() {
        CoreOperator operator = core(CoreOperatorKind.RESTRUCTURE, Ksv.class, Ktv.class);
        assertThat(ProjectionOperatorUtil.isSupported(operator), is(true));
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(dml, operator);
        verify(mappings, "key", "value");
    }

    /**
     * w/ inconsistent type.
     */
    @Test(expected = DiagnosticException.class)
    public void inconsistent_type() {
        CoreOperator operator = core(CoreOperatorKind.RESTRUCTURE, Kv.class, KvInconsistent.class);
        ProjectionOperatorUtil.getPropertyMappings(dml, operator);
    }

    /**
     * project operator.
     */
    @Test(expected = IllegalArgumentException.class)
    public void unsupported() {
        CoreOperator operator = core(CoreOperatorKind.CHECKPOINT, Kv.class, Kv.class);
        ProjectionOperatorUtil.getPropertyMappings(dml, operator);
    }

    private void verify(List<PropertyMapping> mappings, String... properties) {
        Set<PropertyName> expected = new LinkedHashSet<>();
        for (String property : properties) {
            expected.add(PropertyName.of(property));
        }
        Set<PropertyName> actual = new LinkedHashSet<>();
        for (PropertyMapping mapping : mappings) {
            assertThat(mapping.getSourcePort(), is(notNullValue()));
            assertThat(mapping.getDestinationPort(), is(notNullValue()));
            assertThat(mapping.getSourceProperty(), is(mapping.getDestinationProperty()));
            actual.add(mapping.getSourceProperty());
        }
        assertThat(actual, is(expected));
    }

    private CoreOperator core(CoreOperatorKind kind, Class<?> from, Class<?> to) {
        return CoreOperator.builder(kind)
                .input("in", typeOf(from))
                .output("out", typeOf(to))
                .build();
    }

    private UserOperator user() {
        return OperatorExtractor.extract(Update.class, Support.class, "update")
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Kv implements DataModel<Kv> {

        public abstract LongOption getKeyOption();

        public abstract StringOption getValueOption();
    }

    @SuppressWarnings("javadoc")
    public static abstract class KvEquivalent implements DataModel<KvEquivalent> {

        public abstract LongOption getKeyOption();

        public abstract StringOption getValueOption();
    }

    @SuppressWarnings("javadoc")
    public static abstract class KvInconsistent implements DataModel<KvInconsistent> {

        public abstract IntOption getKeyOption();

        public abstract StringOption getValueOption();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ksv implements DataModel<Ksv> {

        public abstract LongOption getKeyOption();

        public abstract DecimalOption getSortOption();

        public abstract StringOption getValueOption();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ktv implements DataModel<Ktv> {

        public abstract LongOption getKeyOption();

        public abstract DateTimeOption getTimeOption();

        public abstract StringOption getValueOption();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Support {

        @Update
        public abstract void update();
    }
}
