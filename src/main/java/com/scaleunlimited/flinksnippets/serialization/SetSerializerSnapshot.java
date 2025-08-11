package com.scaleunlimited.flinksnippets.serialization;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Set;

/** Snapshot class for the {@link SetSerializer}. */
public class SetSerializerSnapshot<T>
        extends CompositeTypeSerializerSnapshot<Set<T>, SetSerializer<T>> {

    private static final int CURRENT_VERSION = 1;

    /** Constructor to create the snapshot for writing. */
    public SetSerializerSnapshot(SetSerializer<T> setSerializer) {
        super(setSerializer);
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected SetSerializer<T> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
        return new SetSerializer<>(elementSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(SetSerializer<T> outerSerializer) {
        return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
    }
}
