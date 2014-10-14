/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.index.util;

import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class with some {@link java.nio.ByteBuffer}/ {@link org.apache.cassandra.db.marshal.AbstractType} utilities.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class ByteBufferUtils
{

    /**
     * Returns the specified {@link java.nio.ByteBuffer} as a byte array.
     *
     * @param byteBuffer a {@link java.nio.ByteBuffer} to be converted to a byte array.
     * @return the byte array representation of the {@code byteBuffer}.
     */
    public static byte[] asArray(ByteBuffer byteBuffer)
    {
        ByteBuffer bb = ByteBufferUtil.clone(byteBuffer);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }

    public static boolean isEmpty(ByteBuffer byteBuffer)
    {
        return byteBuffer.remaining() == 0;
    }

    /**
     * Replaces the last component of the specified {@link java.nio.ByteBuffer} by an
     * {@link org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER} if the {@code byteBuffer} is a composite.
     * Otherwise, replaces {@code byteBuffer} by an empty {@link java.nio.ByteBuffer}.
     *
     * @param byteBuffer the {@link java.nio.ByteBuffer} which last component is to be cleared.
     * @param type       the type of {@code byteBuffer}.
     * @return a {@code byteBuffer} copy with its last component cleared.
     */
    public static ByteBuffer clearLastComponent(ByteBuffer byteBuffer, AbstractType<?> type)
    {
        if (type instanceof CompositeType)
        {
            CompositeType c = (CompositeType) type;
            List<CompositeComponent> components = c.deconstruct(byteBuffer);
            components.remove(components.size() - 1);
            CompositeType.Builder builder = c.builder();
            for (CompositeComponent cc : components)
            {
                builder.add(cc.value);
            }
            builder.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            return builder.build();
        }
        else
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
    }

    /**
     * Removes the last {@link AbstractType} of those contained in {@code comparator}.
     *
     * @param type an {@link AbstractType}.
     * @return the last {@link AbstractType} of those contained in {@code comparator}.
     */
    public static AbstractType<?> removeLastComponent(AbstractType<?> type)
    {
        LinkedList<AbstractType<?>> components = new LinkedList<>(split(type));
        components.removeLast();
        return CompositeType.getInstance(components);
    }

    /**
     * Returns the last {@link java.nio.ByteBuffer} of those contained in {@code byteBuffer} if it is a composite.
     *
     * @param byteBuffer the {@link java.nio.ByteBuffer}.
     * @param type       the {@link AbstractType} of {@code byteBuffer}.
     * @return the last {@link java.nio.ByteBuffer} of those contained in {@code byteBuffer} if it is a composite.
     */
    public static ByteBuffer getLastComponent(ByteBuffer byteBuffer, AbstractType<?> type)
    {
        ByteBuffer[] components = split(byteBuffer, type);
        return components[components.length - 1];
    }

    /**
     * Returns the last {@link AbstractType} of those contained in {@code type}.
     *
     * @param type an {@link AbstractType}.
     * @return the last {@link AbstractType} of those contained in {@code type}.
     */
    public static AbstractType<?> getLastComponent(AbstractType<?> type)
    {
        List<AbstractType<?>> components = split(type);
        return components.get(components.size() - 1);
    }

    /**
     * Returns the {@link AbstractType}s contained in {@code type}.
     *
     * @param type the {@link AbstractType} to be split.
     * @return the {@link AbstractType}s contained in {@code type}.
     */
    public static List<AbstractType<?>> split(AbstractType<?> type)
    {
        if (type instanceof CompositeType)
        {
            return ((CompositeType) type).getComponents();
        }
        else
        {
            List<AbstractType<?>> result = new ArrayList<>(1);
            result.add(type);
            return result;
        }
    }

    /**
     * Returns the {@link java.nio.ByteBuffer}s contained in {@code byteBuffer} according to {@code type}.
     *
     * @param byteBuffer the {@link java.nio.ByteBuffer} to be split.
     * @param type       the {@link AbstractType} of {@code byteBuffer}.
     * @return the {@link java.nio.ByteBuffer}s contained in {@code byteBuffer} according to {@code type}.
     */
    public static ByteBuffer[] split(ByteBuffer byteBuffer, AbstractType<?> type)
    {
        if (type instanceof CompositeType)
        {
            return ((CompositeType) type).split(byteBuffer);
        }
        else
        {
            return new ByteBuffer[]{byteBuffer};
        }
    }

    /**
     * Returns a {@code String} representation of {@code byteBuffer} validated by {@code type}.
     *
     * @param byteBuffer the {@link java.nio.ByteBuffer} to be converted to {@code String}.
     * @param type       {@link AbstractType} of {@code byteBuffer}.
     * @return a {@code String} representation of {@code byteBuffer} validated by {@code type}.
     */
    public static String toString(ByteBuffer byteBuffer, AbstractType<?> type)
    {
        if (type instanceof CompositeType)
        {
            CompositeType composite = (CompositeType) type;
            List<AbstractType<?>> types = composite.types;
            ByteBuffer[] components = composite.split(byteBuffer);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < components.length; i++)
            {
                AbstractType<?> componentType = types.get(i);
                ByteBuffer component = components[i];
                sb.append(componentType.compose(component));
                if (i < types.size() - 1)
                {
                    sb.append(':');
                }
            }
            return sb.toString();
        }
        else
        {
            return type.compose(byteBuffer).toString();
        }
    }

    /**
     * Returns a {@code String} representation of {@link byteBuffer}.
     *
     * @param byteBuffer the {@link java.nio.ByteBuffer} to be converted to {@link String}.
     * @return a {@code String} representation of {@link byteBuffer}.
     */
    public static String toString(ByteBuffer byteBuffer)
    {
        return Base256Serializer.string(byteBuffer);
    }

    /**
     * Returns the {@link java.nio.ByteBuffer} represented by {@code string}, which must be have generated by
     * {@link #toString(ByteBuffer)}.
     *
     * @param string the {@link String} to be converted to {@link ByteBuffer}. This must be have generated by
     *               {@link #toString(ByteBuffer)}.
     * @return the {@link java.nio.ByteBuffer} represented by {@code string}.
     */
    public static ByteBuffer fromString(String string)
    {
        return Base256Serializer.byteBuffer(string);
    }

    public static String toHex(ByteBuffer byteBuffer)
    {
        return ByteBufferUtil.bytesToHex(byteBuffer);
    }

    public static String toHex(byte[] bytes)
    {
        return Hex.bytesToHex(bytes);
    }

}