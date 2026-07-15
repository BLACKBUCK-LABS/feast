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
package feast.serving.connectors.redis.retriever;

import com.google.common.collect.Lists;
import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.serving.connectors.Feature;
import feast.serving.connectors.OnlineRetriever;
import feast.serving.connectors.redis.common.RedisHashDecoder;
import feast.serving.connectors.redis.common.RedisKeyGenerator;
import feast.serving.service.config.ServingServiceV2Module;
import io.lettuce.core.KeyValue;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class RedisOnlineRetriever implements OnlineRetriever {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisOnlineRetriever.class);

  private static final String timestampPrefix = "_ts";
  private final RedisClientAdapter redisClientAdapter;
  private final EntityKeySerializer keySerializer;
  private final String project;

  // Number of fields in request to Redis which requires using HGETALL instead of HMGET.
  // Kept high on purpose: HMGET transfers only the requested fields (absent ones come back
  // as cheap nulls), while HGETALL returns the ENTIRE hash - for entity keys shared by many
  // feature views that's strictly more network bytes and decode work than the request needs.
  // The upstream value of 50 was tuned on a different workload; here HGETALL only wins for
  // pathological field counts.
  public static final int HGETALL_NUMBER_OF_FIELDS_THRESHOLD = 500;

  public RedisOnlineRetriever(
      String project, RedisClientAdapter redisClientAdapter, EntityKeySerializer keySerializer) {
    this.project = project;
    this.redisClientAdapter = redisClientAdapter;
    this.keySerializer = keySerializer;
  }

  @Override
  public List<List<Feature>> getOnlineFeatures(
      List<Map<String, ValueProto.Value>> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<String> entityNames) {

    List<RedisProto.RedisKeyV2> redisKeys =
        RedisKeyGenerator.buildRedisKeys(this.project, entityRows);
    return getFeaturesFromRedis(redisKeys, featureReferences);
  }

  private List<List<Feature>> getFeaturesFromRedis(
      List<RedisProto.RedisKeyV2> redisKeys,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    // To decode bytes back to Feature
    Map<ByteBuffer, Integer> byteToFeatureIdxMap = new HashMap<>();

    // Serialize using proto
    List<byte[]> binaryRedisKeys =
        redisKeys.stream().map(this.keySerializer::serialize).collect(Collectors.toList());

    List<byte[]> retrieveFields = new ArrayList<>();
    for (int idx = 0;
        idx < featureReferences.size();
        idx++) { // eg. murmur(<featuretable_name:feature_name>)
      byte[] featureReferenceBytes =
          RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReferences.get(idx));
      retrieveFields.add(featureReferenceBytes);

      byteToFeatureIdxMap.put(ByteBuffer.wrap(featureReferenceBytes), idx);
    }

    featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureViewName)
        .distinct()
        .forEach(
            table -> {
              // eg. <_ts:featuretable_name>
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(table, timestampPrefix);

              retrieveFields.add(featureTableTsBytes);
            });

    // Decode is chained into each future via thenApplyAsync so each entity row is decoded as
    // soon as its Redis response lands - in parallel, off the netty event loop (thenApply here
    // would run the collect/decode on the I/O thread and delay other in-flight responses), and
    // NOT on the group-fetch executor (its threads block on allOf below; queueing decode stages
    // behind them on the same bounded pool could deadlock). Default async executor = common pool.
    List<CompletableFuture<List<Feature>>> futures =
        Lists.newArrayListWithExpectedSize(binaryRedisKeys.size());

    if (retrieveFields.size() < HGETALL_NUMBER_OF_FIELDS_THRESHOLD) {
      byte[][] retrieveFieldsByteArray = retrieveFields.toArray(new byte[0][]);

      for (byte[] binaryRedisKey : binaryRedisKeys) {
        futures.add(
            redisClientAdapter
                .hmget(binaryRedisKey, retrieveFieldsByteArray)
                .toCompletableFuture()
                .thenApplyAsync(
                    list ->
                        RedisHashDecoder.retrieveFeature(
                            list.stream()
                                .filter(KeyValue::hasValue)
                                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)),
                            byteToFeatureIdxMap,
                            featureReferences,
                            timestampPrefix)));
      }
    } else {
      for (byte[] binaryRedisKey : binaryRedisKeys) {
        futures.add(
            redisClientAdapter
                .hgetall(binaryRedisKey)
                .toCompletableFuture()
                .thenApplyAsync(
                    map ->
                        RedisHashDecoder.retrieveFeature(
                            map, byteToFeatureIdxMap, featureReferences, timestampPrefix)));
      }
    }

    // force all queued commands over the wire in a single TCP write instead of waiting for Netty
    // auto-flush
    if (binaryRedisKeys.size() > 1) {
      redisClientAdapter.flushCommands();
      if (ThreadLocalRandom.current().nextInt(1000) == 0) {
        log.debug("REDIS_FLUSH batched={} fields={}", binaryRedisKeys.size(), retrieveFields.size());
      }
    }

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("Exception occurred while fetching features from redis {}", e.getMessage(), e);
      throw new RuntimeException("Unexpected error when pulling data from Redis");
    }

    List<List<Feature>> results = Lists.newArrayListWithExpectedSize(futures.size());
    for (CompletableFuture<List<Feature>> f : futures) {
      results.add(f.join());
    }

    return results;
  }
}
