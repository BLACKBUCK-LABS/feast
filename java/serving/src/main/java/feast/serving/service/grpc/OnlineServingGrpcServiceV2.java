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
package feast.serving.service.grpc;

import com.newrelic.api.agent.NewRelic;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.serving.service.ServingServiceV2;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;

public class OnlineServingGrpcServiceV2 extends ServingServiceGrpc.ServingServiceImplBase {
  private final ServingServiceV2 servingServiceV2;
  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(OnlineServingGrpcServiceV2.class);

  @Inject
  OnlineServingGrpcServiceV2(ServingServiceV2 servingServiceV2) {
    this.servingServiceV2 = servingServiceV2;
  }

  @Override
  public void getFeastServingInfo(
      ServingAPIProto.GetFeastServingInfoRequest request,
      StreamObserver<ServingAPIProto.GetFeastServingInfoResponse> responseObserver) {
    try {
      responseObserver.onNext(this.servingServiceV2.getFeastServingInfo(request));
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.warn("Failed to get Serving Info", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  private static final long SLOW_REQUEST_THRESHOLD_MS = 500;

  @Override
  public void getOnlineFeatures(
      ServingAPIProto.GetOnlineFeaturesRequest request,
      StreamObserver<ServingAPIProto.GetOnlineFeaturesResponse> responseObserver) {
    long startNs = System.nanoTime();
    try {
      int entityCount = request.getEntitiesMap().isEmpty() ? 0
          : request.getEntitiesMap().values().iterator().next().getValCount();
      NewRelic.addCustomParameter("entity_count", entityCount);
      NewRelic.addCustomParameter("feature_service", request.getFeatureService());
      NewRelic.addCustomParameter("features_requested", request.getFeatures().getValCount());

      ServingAPIProto.GetOnlineFeaturesResponse response = this.servingServiceV2.getOnlineFeatures(request);

      long totalMs = (System.nanoTime() - startNs) / 1_000_000;
      NewRelic.addCustomParameter("total_ms", totalMs);
      log.info("REQUEST_DONE feature_service={} entity_count={} total_ms={}",
          request.getFeatureService(), entityCount, totalMs);

      if (totalMs >= SLOW_REQUEST_THRESHOLD_MS) {
        Map<String, Object> slowEvent = new HashMap<>();
        slowEvent.put("feature_service", request.getFeatureService());
        slowEvent.put("entity_count", entityCount);
        slowEvent.put("features_requested", request.getFeatures().getValCount());
        slowEvent.put("total_ms", totalMs);
        NewRelic.getAgent().getInsights().recordCustomEvent("SlowFeatureRequest", slowEvent);
        log.warn("SLOW_REQUEST feature_service={} entity_count={} total_ms={}",
            request.getFeatureService(), entityCount, totalMs);
      }

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.warn("Failed to get Online Features", e);
      NewRelic.noticeError(e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
