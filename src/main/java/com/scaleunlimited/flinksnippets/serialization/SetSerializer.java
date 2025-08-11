package com.scaleunlimited.flinksnippets.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A serializer for {@link Set}. The serializer relies on an element serializer.
 *
 * <p>The serialization format for the set is as follows: four bytes for the size of the set,
 * followed by the serialized representation of each entry. Nulls are not allowed
 *
 * @param <E> The type of the entries in the set.
 */
@Internal
public final class SetSerializer<E> extends TypeSerializer<Set<E>> {

    private static final long serialVersionUID = -6885593032367050078L;

    /** The serializer for the elements in the set */
    private final TypeSerializer<E> elementSerializer;

    /**
     * Creates a set serializer that uses the given serializer to serialize the entries in the set.
     *
     * @param entrySerializer The serializer for the entries in the set
     */
    public SetSerializer(TypeSerializer<E> entrySerializer) {
        this.elementSerializer =
                Preconditions.checkNotNull(entrySerializer, "The entry serializer cannot be null");
    }

    // ------------------------------------------------------------------------
    //  SetSerializer specific properties
    // ------------------------------------------------------------------------

    public TypeSerializer<E> getElementSerializer() {
        return elementSerializer;
    }

    // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<E>> duplicate() {
        TypeSerializer<E> duplicateEntrySerializer = elementSerializer.duplicate();

        return (duplicateEntrySerializer == elementSerializer)
                ? this
                : new SetSerializer<>(duplicateEntrySerializer);
    }

    @Override
    public Set<E> createInstance() {
        return new HashSet<>();
    }

    @Override
    public Set<E > copy(Set<E> from) {
        return new HashSet<>(from);
    }

    @Override
    public Set<E> copy(Set<E> from, Set<E> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(Set<E> set, DataOutputView target) throws IOException {
        final int size = set.size();
        target.writeInt(size);

        for (E entry : set) {
            elementSerializer.serialize(entry, target);
        }
    }

    @Override
    public Set<E> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();

        final Set<E> set = CollectionUtil.newHashSetWithExpectedSize(size);
        
        for (int i = 0; i < size; ++i) {
            set.add(elementSerializer.deserialize(source));
        }

        return set;
    }

    @Override
    public Set<E> deserialize(Set<E> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int size = source.readInt();
        target.writeInt(size);

        for (int i = 0; i < size; ++i) {
            elementSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(((SetSerializer<?>)obj).getElementSerializer()));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Set<E>> snapshotConfiguration() {
        return new SetSerializerSnapshot<>(this);
    }
}
