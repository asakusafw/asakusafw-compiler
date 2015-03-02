package com.asakusafw.lang.compiler.core.adapter;

import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.core.AnalyzerContext;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * An adapter for {@link DataModelLoader}.
 */
public class DataModelLoaderAdapter implements DataModelLoader {

    private final DataModelProcessor processor;

    private final DataModelProcessor.Context context;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     */
    public DataModelLoaderAdapter(AnalyzerContext delegate) {
        this.processor = delegate.getTools().getDataModelProcessor();
        this.context = new DataModelProcessorAdapter(delegate);
    }

    /**
     * Creates a new instance.
     * @param processor the internal data model processor
     * @param context the data model processor's context
     */
    public DataModelLoaderAdapter(DataModelProcessor processor, DataModelProcessor.Context context) {
        this.processor = processor;
        this.context = context;
    }

    @Override
    public DataModelReference load(TypeDescription type) {
        DataModelReference cached = Cache.get(context, type);
        if (cached != null) {
            return cached;
        }
        DataModelReference result = processor.process(context, type);
        Cache.put(context, type, result);
        return result;
    }

    private static final class Cache {

        private final Map<TypeDescription, DataModelReference> entries = new HashMap<>();

        private static Cache get(DataModelProcessor.Context context) {
            if (context instanceof ExtensionContainer.Editable) {
                ExtensionContainer.Editable extensions = (ExtensionContainer.Editable) context;
                Cache extension = extensions.getExtension(Cache.class);
                if (extension == null) {
                    extension = new Cache();
                    extensions.registerExtension(Cache.class, extension);
                }
                return extension;
            }
            return null;
        }

        public static DataModelReference get(DataModelProcessor.Context context, TypeDescription type) {
            Cache cache = get(context);
            if (cache == null) {
                return null;
            }
            return cache.entries.get(type);
        }

        public static void put(DataModelProcessor.Context context, TypeDescription type, DataModelReference value) {
            Cache cache = get(context);
            if (cache == null) {
                return;
            }
            cache.entries.put(type, value);
        }
    }
}
