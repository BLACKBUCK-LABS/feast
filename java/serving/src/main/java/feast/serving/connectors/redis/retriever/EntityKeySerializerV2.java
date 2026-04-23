/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.connectors.redis.retriever;

import com.google.protobuf.ProtocolStringList;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This is derived from
// https://github.com/feast-dev/feast/blob/b1ccf8dd1535f721aee8bea937ee38feff80bec5/sdk/python/feast/infra/key_encoding_utils.py#L22
// and must be kept up to date with any changes in that logic.
public class EntityKeySerializerV2 implements EntityKeySerializer {
  private static final Logger log = LoggerFactory.getLogger(EntityKeySerializerV2.class);

  // sampling counter — log comparison for 1 in 1000 keys to verify correctness without log flood
  private static final int COMPARISON_SAMPLE_RATE = 1000;
  private static long serializeCallCount = 0;

  private final int entityKeySerializationVersion;

  public EntityKeySerializerV2() {
    this(1);
  }

  public EntityKeySerializerV2(int entityKeySerializationVersion) {
    this.entityKeySerializationVersion = entityKeySerializationVersion;
  }

  @Override
  public byte[] serialize(RedisProto.RedisKeyV2 entityKey) {
    byte[] newBytes = serializeFast(entityKey);

    serializeCallCount++;
    if (serializeCallCount % COMPARISON_SAMPLE_RATE == 0) {
      byte[] oldBytes = serializeLegacy(entityKey);
      if (!Arrays.equals(oldBytes, newBytes)) {
        log.error("SERIALIZER MISMATCH entity={} old={} new={}",
            entityKey.getEntityNamesList(),
            Arrays.toString(oldBytes),
            Arrays.toString(newBytes));
      } else {
        log.info("SERIALIZER OK sample={} entity={}", serializeCallCount, entityKey.getEntityNamesList());
      }
    }

    return newBytes;
  }

  private byte[] serializeFast(RedisProto.RedisKeyV2 entityKey) {
    final ProtocolStringList joinKeys = entityKey.getEntityNamesList();
    final List<ValueProto.Value> values = entityKey.getEntityValuesList();

    final List<Pair<String, ValueProto.Value>> tuples = new ArrayList<>(joinKeys.size());
    for (int i = 0; i < joinKeys.size(); i++) {
      tuples.add(Pair.of(joinKeys.get(i), values.get(i)));
    }
    tuples.sort(Comparator.comparing(Pair::getLeft));

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    for (Pair<String, ValueProto.Value> pair : tuples) {
      writeInteger(out, ValueProto.ValueType.Enum.STRING.getNumber());
      writeString(out, pair.getLeft());
    }

    for (Pair<String, ValueProto.Value> pair : tuples) {
      final ValueProto.Value val = pair.getRight();
      switch (val.getValCase()) {
        case STRING_VAL:
          String stringVal = val.getStringVal();
          writeInteger(out, ValueProto.ValueType.Enum.STRING.getNumber());
          writeInteger(out, stringVal.length());
          writeString(out, stringVal);
          break;
        case BYTES_VAL:
          byte[] bytes = val.getBytesVal().toByteArray();
          writeInteger(out, ValueProto.ValueType.Enum.BYTES.getNumber());
          writeInteger(out, bytes.length);
          writeBytes(out, bytes);
          break;
        case INT32_VAL:
          writeInteger(out, ValueProto.ValueType.Enum.INT32.getNumber());
          writeInteger(out, Integer.BYTES);
          writeInteger(out, val.getInt32Val());
          break;
        case INT64_VAL:
          writeInteger(out, ValueProto.ValueType.Enum.INT64.getNumber());
          if (this.entityKeySerializationVersion <= 1) {
            writeInteger(out, Integer.BYTES);
            writeInteger(out, ((Long) val.getInt64Val()).intValue());
          } else {
            writeInteger(out, Long.BYTES);
            writeLong(out, val.getInt64Val());
          }
          break;
        default:
          throw new RuntimeException("Unable to serialize Entity Key");
      }
    }

    writeString(out, entityKey.getProject());
    return out.toByteArray();
  }

  // kept for comparison logging only — remove once verified in prod
  private byte[] serializeLegacy(RedisProto.RedisKeyV2 entityKey) {
    final ProtocolStringList joinKeys = entityKey.getEntityNamesList();
    final List<ValueProto.Value> values = entityKey.getEntityValuesList();

    final List<Byte> buffer = new ArrayList<>();
    final List<Pair<String, ValueProto.Value>> tuples = new ArrayList<>(joinKeys.size());
    for (int i = 0; i < joinKeys.size(); i++) {
      tuples.add(Pair.of(joinKeys.get(i), values.get(i)));
    }
    tuples.sort(Comparator.comparing(Pair::getLeft));

    for (Pair<String, ValueProto.Value> pair : tuples) {
      buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
      buffer.addAll(encodeString(pair.getLeft()));
    }
    for (Pair<String, ValueProto.Value> pair : tuples) {
      final ValueProto.Value val = pair.getRight();
      switch (val.getValCase()) {
        case STRING_VAL:
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.STRING.getNumber()));
          buffer.addAll(encodeInteger(val.getStringVal().length()));
          buffer.addAll(encodeString(val.getStringVal()));
          break;
        case BYTES_VAL:
          byte[] bytes = val.getBytesVal().toByteArray();
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.BYTES.getNumber()));
          buffer.addAll(encodeInteger(bytes.length));
          buffer.addAll(encodeBytes(bytes));
          break;
        case INT32_VAL:
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT32.getNumber()));
          buffer.addAll(encodeInteger(Integer.BYTES));
          buffer.addAll(encodeInteger(val.getInt32Val()));
          break;
        case INT64_VAL:
          buffer.addAll(encodeInteger(ValueProto.ValueType.Enum.INT64.getNumber()));
          if (this.entityKeySerializationVersion <= 1) {
            buffer.addAll(encodeInteger(Integer.BYTES));
            buffer.addAll(encodeInteger(((Long) val.getInt64Val()).intValue()));
          } else {
            buffer.addAll(encodeInteger(Long.BYTES));
            buffer.addAll(encodeLong(val.getInt64Val()));
          }
          break;
        default:
          throw new RuntimeException("Unable to serialize Entity Key");
      }
    }
    buffer.addAll(encodeString(entityKey.getProject()));
    final byte[] result = new byte[buffer.size()];
    for (int i = 0; i < buffer.size(); i++) result[i] = buffer.get(i);
    return result;
  }

  private void writeBytes(ByteArrayOutputStream out, byte[] bytes) {
    out.write(bytes, 0, bytes.length);
  }

  private void writeInteger(ByteArrayOutputStream out, int value) {
    out.write(value & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 24) & 0xFF);
  }

  private void writeLong(ByteArrayOutputStream out, long value) {
    out.write((int) (value & 0xFF));
    out.write((int) ((value >> 8) & 0xFF));
    out.write((int) ((value >> 16) & 0xFF));
    out.write((int) ((value >> 24) & 0xFF));
    out.write((int) ((value >> 32) & 0xFF));
    out.write((int) ((value >> 40) & 0xFF));
    out.write((int) ((value >> 48) & 0xFF));
    out.write((int) ((value >> 56) & 0xFF));
  }

  private void writeString(ByteArrayOutputStream out, String value) {
    writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
  }

  private List<Byte> encodeBytes(byte[] toByteArray) {
    return Arrays.asList(ArrayUtils.toObject(toByteArray));
  }

  private List<Byte> encodeInteger(Integer value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(value);
    return Arrays.asList(ArrayUtils.toObject(buffer.array()));
  }

  private List<Byte> encodeLong(Long value) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(value);
    return Arrays.asList(ArrayUtils.toObject(buffer.array()));
  }

  private List<Byte> encodeString(String value) {
    return encodeBytes(value.getBytes(StandardCharsets.UTF_8));
  }
}
