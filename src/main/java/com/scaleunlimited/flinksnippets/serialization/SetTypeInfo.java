package com.scaleunlimited.flinksnippets.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code TypeInformation} used for sets
 *
 * @param <E> The type of the elements in the set.
 */
@SuppressWarnings("serial")
public class SetTypeInfo<E> extends TypeInformation<Set<E>> {

    /* The type information for the elements in the set*/
    private final TypeInformation<E> elementTypeInfo;

    public SetTypeInfo(TypeInformation<E> elementTypeInfo) {
        this.elementTypeInfo =
                Preconditions.checkNotNull(elementTypeInfo, "The element type information cannot be null.");
    }

    public SetTypeInfo(Class<E> elementClass) {
        this.elementTypeInfo = of(checkNotNull(elementClass, "The element class cannot be null."));
    }

    // ------------------------------------------------------------------------
    //  SetTypeInfo specific properties
    // ------------------------------------------------------------------------

    /** Gets the type information for the elements in the set */
    public TypeInformation<E> getElementTypeInfo() {
        return elementTypeInfo;
    }

    // ------------------------------------------------------------------------
    //  TypeInformation implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Set<E>> getTypeClass() {
        return (Class<Set<E>>) (Class<?>) Set.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<E>> createSerializer(SerializerConfig config) {
        TypeSerializer<E> elementTypeSerializer = elementTypeInfo.createSerializer(config);

        return new SetSerializer<>(elementTypeSerializer);
    }

    @Override
    public TypeSerializer<Set<E>> createSerializer(ExecutionConfig config) {
        return createSerializer(config.getSerializerConfig());
    }

    @Override
    public String toString() {
        return "Set<" + elementTypeInfo + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof SetTypeInfo) {
            @SuppressWarnings("unchecked")
            SetTypeInfo<E> other = (SetTypeInfo<E>) obj;

            return (other.canEqual(this)
                    && elementTypeInfo.equals(other.elementTypeInfo));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return elementTypeInfo.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return (obj != null && obj.getClass() == getClass());
    }
}