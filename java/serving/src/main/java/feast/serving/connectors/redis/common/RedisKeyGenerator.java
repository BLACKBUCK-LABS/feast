/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.serving.connectors.redis.common;

import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisKeyGenerator {

  public static List<RedisProto.RedisKeyV2> buildRedisKeys(
      String project, List<Map<String, ValueProto.Value>> entityRows) {
    if (entityRows.isEmpty()) return new ArrayList<>();

    // sort entity names once — same keys in every row
    List<String> sortedEntityNames = new ArrayList<>(entityRows.get(0).keySet());
    sortedEntityNames.sort(String::compareTo);

    List<RedisProto.RedisKeyV2> redisKeys = new ArrayList<>(entityRows.size());
    for (Map<String, ValueProto.Value> entityRow : entityRows) {
      redisKeys.add(makeRedisKey(project, entityRow, sortedEntityNames));
    }
    return redisKeys;
  }

  private static RedisProto.RedisKeyV2 makeRedisKey(
      String project, Map<String, ValueProto.Value> entityRow, List<String> sortedEntityNames) {
    RedisProto.RedisKeyV2.Builder builder = RedisProto.RedisKeyV2.newBuilder().setProject(project);
    for (String entityName : sortedEntityNames) {
      builder.addEntityNames(entityName);
      builder.addEntityValues(entityRow.get(entityName));
    }
    return builder.build();
  }
}
