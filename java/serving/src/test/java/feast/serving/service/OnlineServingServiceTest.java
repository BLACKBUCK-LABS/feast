/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.serving.service;

import static feast.serving.util.DataGenerator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.types.ValueProto;
import feast.serving.connectors.Feature;
import feast.serving.connectors.ProtoFeature;
import feast.serving.connectors.redis.retriever.RedisOnlineRetriever;
import feast.serving.registry.Registry;
import feast.serving.registry.RegistryRepository;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class OnlineServingServiceTest {

  @Mock Registry registry;
  @Mock Tracer tracer;
  @Mock RedisOnlineRetriever retrieverV2;
  private String transformationServiceEndpoint;

  private OnlineServingServiceV2 onlineServingServiceV2;

  List<Feature> mockedFeatureRows;
  List<FeatureProto.FeatureSpecV2> featureSpecs;

  Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

  @Before
  public void setUp() {
    initMocks(this);

    RegistryRepository registryRepo = new RegistryRepository(registry);

    OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(transformationServiceEndpoint, registryRepo);
    onlineServingServiceV2 =
        new OnlineServingServiceV2(
            retrieverV2,
            registryRepo,
            onlineTransformationService,
            "feast_project",
            Optional.of(tracer));

    mockedFeatureRows = new ArrayList<>();
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            now,
            createStrValue("1")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_2")
                .build(),
            now,
            createStrValue("2")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            now,
            createStrValue("3")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_2")
                .build(),
            now,
            createStrValue("4")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_3")
                .build(),
            now,
            createStrValue("5")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(1).build(),
            createStrValue("6")));

    featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureProto.FeatureSpecV2.newBuilder()
            .setName("feature_1")
            .setValueType(ValueProto.ValueType.Enum.STRING)
            .build());
    featureSpecs.add(
        FeatureProto.FeatureSpecV2.newBuilder()
            .setName("feature_2")
            .setValueType(ValueProto.ValueType.Enum.STRING)
            .build());
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfKeysPresent() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(0), mockedFeatureRows.get(1)),
            List.of(mockedFeatureRows.get(2), mockedFeatureRows.get(3)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any())).thenReturn(getFeatureViewSpec());
    when(registry.getFeatureSpec(mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getFeatureSpec(mockedFeatureRows.get(2).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(3).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getEntityJoinKey("entity1")).thenReturn("entity1");
    when(registry.getEntityJoinKey("entity2")).thenReturn("entity2");

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("1"))
                    .addValues(createStrValue("3"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("4"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    ServingAPIProto.GetOnlineFeaturesResponse actual =
        onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfKeysNotPresent() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(4));

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(0), mockedFeatureRows.get(1)),
            Arrays.asList(null, mockedFeatureRows.get(4)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any())).thenReturn(getFeatureViewSpec());
    when(registry.getFeatureSpec(mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getEntityJoinKey("entity1")).thenReturn("entity1");
    when(registry.getEntityJoinKey("entity2")).thenReturn("entity2");

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("1"))
                    .addValues(createEmptyValue())
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.NOT_FOUND)
                    .addEventTimestamps(now)
                    .addEventTimestamps(Timestamp.newBuilder().build()))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("5"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfMaxAgeIsExceeded() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(5), mockedFeatureRows.get(1)),
            List.of(mockedFeatureRows.get(5), mockedFeatureRows.get(1)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any()))
        .thenReturn(
            FeatureViewProto.FeatureViewSpec.newBuilder()
                .setName("featureview_1")
                .addEntities("entity1")
                .addEntities("entity2")
                .addFeatures(
                    FeatureProto.FeatureSpecV2.newBuilder()
                        .setName("feature_1")
                        .setValueType(ValueProto.ValueType.Enum.STRING)
                        .build())
                .addFeatures(
                    FeatureProto.FeatureSpecV2.newBuilder()
                        .setName("feature_2")
                        .setValueType(ValueProto.ValueType.Enum.STRING)
                        .build())
                .setTtl(Duration.newBuilder().setSeconds(3600))
                .build());
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getFeatureSpec(mockedFeatureRows.get(5).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getEntityJoinKey("entity1")).thenReturn("entity1");
    when(registry.getEntityJoinKey("entity2")).thenReturn("entity2");

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("6"))
                    .addValues(createStrValue("6"))
                    .addStatuses(FieldStatus.OUTSIDE_MAX_AGE)
                    .addStatuses(FieldStatus.OUTSIDE_MAX_AGE)
                    .addEventTimestamps(Timestamp.newBuilder().setSeconds(1).build())
                    .addEventTimestamps(Timestamp.newBuilder().setSeconds(1).build()))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("2"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  // Guards the parallel cross-group fetch in retrieveFeatures(): two feature views with
  // different entities land in different join-key groups and are fetched concurrently on
  // separate threads. This asserts the merge puts each group's results back at the correct
  // column index regardless of which thread's future completes first - a regression here
  // would silently swap feature values between columns while still returning 200 OK.
  @Test
  public void shouldMergeMultipleJoinKeyGroupsAtCorrectColumns() {
    ServingAPIProto.FeatureReferenceV2 fv1Feature1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 fv2Feature1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_2")
            .setFeatureName("feature_1")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(fv1Feature1, fv2Feature1);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    FeatureViewProto.FeatureViewSpec fv1Spec =
        FeatureViewProto.FeatureViewSpec.newBuilder()
            .setName("featureview_1")
            .addEntities("entity1")
            .addFeatures(
                FeatureProto.FeatureSpecV2.newBuilder()
                    .setName("feature_1")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .setTtl(Duration.newBuilder().setSeconds(3600))
            .build();
    FeatureViewProto.FeatureViewSpec fv2Spec =
        FeatureViewProto.FeatureViewSpec.newBuilder()
            .setName("featureview_2")
            .addEntities("entity2")
            .addFeatures(
                FeatureProto.FeatureSpecV2.newBuilder()
                    .setName("feature_1")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .setTtl(Duration.newBuilder().setSeconds(3600))
            .build();

    Feature fv1Row0 =
        new ProtoFeature(fv1Feature1, now, createStrValue("fv1-row0"));
    Feature fv1Row1 =
        new ProtoFeature(fv1Feature1, now, createStrValue("fv1-row1"));
    Feature fv2Row0 =
        new ProtoFeature(fv2Feature1, now, createStrValue("fv2-row0"));
    Feature fv2Row1 =
        new ProtoFeature(fv2Feature1, now, createStrValue("fv2-row1"));

    // getJoinKeyGroupForFeatureView (called before fan-out, to build the groups) constructs its
    // own FeatureReferenceV2 with only featureViewName set - match on that field, not object
    // equality, so both that lookup and fetchGroup's per-group lookup resolve correctly.
    when(registry.getFeatureViewSpec(any()))
        .thenAnswer(
            invocation -> {
              ServingAPIProto.FeatureReferenceV2 ref = invocation.getArgument(0);
              return ref.getFeatureViewName().equals("featureview_1") ? fv1Spec : fv2Spec;
            });
    when(registry.getFeatureSpec(fv1Feature1)).thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(fv2Feature1)).thenReturn(featureSpecs.get(0));
    when(registry.getEntityJoinKey("entity1")).thenReturn("entity1");
    when(registry.getEntityJoinKey("entity2")).thenReturn("entity2");

    // Distinguish which group a call belongs to by the featureReferences argument, since both
    // groups now fetch concurrently rather than in a guaranteed call order.
    when(retrieverV2.getOnlineFeatures(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              List<ServingAPIProto.FeatureReferenceV2> refs = invocation.getArgument(1);
              if (refs.contains(fv1Feature1)) {
                return List.of(List.of(fv1Row0), List.of(fv1Row1));
              }
              return List.of(List.of(fv2Row0), List.of(fv2Row1));
            });

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);

    // Column 0 must be featureview_1's values, column 1 must be featureview_2's - never swapped.
    assertThat(
        actual.getResults(0).getValues(0).getStringVal(),
        equalTo("fv1-row0"));
    assertThat(
        actual.getResults(0).getValues(1).getStringVal(),
        equalTo("fv1-row1"));
    assertThat(
        actual.getResults(1).getValues(0).getStringVal(),
        equalTo("fv2-row0"));
    assertThat(
        actual.getResults(1).getValues(1).getStringVal(),
        equalTo("fv2-row1"));
    assertThat(
        actual.getMetadata().getFeatureNames().getVal(0),
        equalTo("featureview_1:feature_1"));
    assertThat(
        actual.getMetadata().getFeatureNames().getVal(1),
        equalTo("featureview_2:feature_1"));
  }

  private FeatureViewProto.FeatureViewSpec getFeatureViewSpec() {
    return FeatureViewProto.FeatureViewSpec.newBuilder()
        .setName("featureview_1")
        .addEntities("entity1")
        .addEntities("entity2")
        .addFeatures(
            FeatureProto.FeatureSpecV2.newBuilder()
                .setName("feature_1")
                .setValueType(ValueProto.ValueType.Enum.STRING)
                .build())
        .addFeatures(
            FeatureProto.FeatureSpecV2.newBuilder()
                .setName("feature_2")
                .setValueType(ValueProto.ValueType.Enum.STRING)
                .build())
        .setTtl(Duration.newBuilder().setSeconds(120))
        .build();
  }

  private ServingAPIProto.GetOnlineFeaturesRequest getOnlineFeaturesRequest(
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return ServingAPIProto.GetOnlineFeaturesRequest.newBuilder()
        .setFeatures(
            ServingAPIProto.FeatureList.newBuilder()
                .addAllVal(
                    featureReferences.stream()
                        .map(FeatureUtil::getFeatureReference)
                        .collect(Collectors.toList()))
                .build())
        .putAllEntities(
            ImmutableMap.of(
                "entity1",
                    ValueProto.RepeatedValue.newBuilder()
                        .addAllVal(List.of(createInt64Value(1), createInt64Value(2)))
                        .build(),
                "entity2",
                    ValueProto.RepeatedValue.newBuilder()
                        .addAllVal(List.of(createStrValue("a"), createStrValue("b")))
                        .build()))
        .build();
  }
}
