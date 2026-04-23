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
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

// This is derived from
// https://github.com/feast-dev/feast/blob/b1ccf8dd1535f721aee8bea937ee38feff80bec5/sdk/python/feast/infra/key_encoding_utils.py#L22
// and must be kept up to date with any changes in that logic.
public class EntityKeySerializerV2 implements EntityKeySerializer {

  private final int entityKeySerializationVersion;

  public EntityKeySerializerV2() {
    this(1);
  }

  public EntityKeySerializerV2(int entityKeySerializationVersion) {
    this.entityKeySerializationVersion = entityKeySerializationVersion;
  }

  @Override
  public byte[] serialize(RedisProto.RedisKeyV2 entityKey) {
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
}
