/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.zendesk.source.batch.http;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import io.cdap.plugin.zendesk.source.common.ObjectType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PagedIteratorTest {

  @Test
  public void testGetNextPage() throws IOException {
    String expected = "expected_page";

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", expected));
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetNextPageNull() throws IOException {
    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(new HashMap<>());
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatch() throws IOException {
    String expected = "expected_page";
    long endTime = TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6));
    int count = 1000;

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", expected,
                                                                "end_time", endTime,
                                                                "count", count));
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetNextPageBatchNull() throws IOException {
    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(new HashMap<>());
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchEndTimeNull() throws IOException {
    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page"));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchEndTimeZero() throws IOException {
    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page",
                                                                "end_time", 0));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchEndTime1MinuteAgo() throws IOException {
    long endTime = TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(4));

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page",
                                                                "end_time", endTime));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchCountNull() throws IOException {
    long endTime = TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6));

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page",
                                                                "end_time", endTime));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchCountZero() throws IOException {
    long endTime = TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6));

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page",
                                                                "end_time", endTime,
                                                                "count", 0));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetNextPageBatchCountLessThan1000() throws IOException {
    long endTime = TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6));
    int count = 999;

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      String actual = pagedIterator.getNextPage(ImmutableMap.of("next_page", "expected_page",
                                                                "end_time", endTime,
                                                                "count", count));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetJsonValuesFromResponse() throws IOException {
    ImmutableMap<String, Object> response = ImmutableMap.of(
      "organizations", Arrays.asList(
        new HashMap<>(ImmutableMap.of("key", "val1")),
        new HashMap<>(ImmutableMap.of("key", "val2"))));
    List<String> expected = new ArrayList<>();
    expected.add("{\"key\":\"val1\",\"object\":\"Organizations\"}");
    expected.add("{\"key\":\"val2\",\"object\":\"Organizations\"}");

    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Iterator<String> result = pagedIterator.getJsonValuesFromResponse(response);
      List<String> actual = new ArrayList<>();
      result.forEachRemaining(actual::add);

      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetJsonValuesFromResponseWithChildKey() throws IOException {
    ImmutableMap<String, Object> response = ImmutableMap.of(
      "ticket_events",
      Collections.singletonList(ImmutableMap.of(
        "child_events", Arrays.asList(
          new HashMap<>(ImmutableMap.of("key", "val1", "event_type", "Comment")),
          new HashMap<>(ImmutableMap.of("key", "val2", "event_type", "Comment")),
          new HashMap<>(ImmutableMap.of("key", "val3", "event_type", "Not Comment"))))));
    List<String> expected = new ArrayList<>();
    expected.add("{\"key\":\"val1\",\"event_type\":\"Comment\",\"object\":\"Ticket Comments\"}");
    expected.add("{\"key\":\"val2\",\"event_type\":\"Comment\",\"object\":\"Ticket Comments\"}");

    ObjectType objectType = ObjectType.TICKET_COMMENTS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Iterator<String> result = pagedIterator.getJsonValuesFromResponse(response);
      List<String> actual = new ArrayList<>();
      result.forEachRemaining(actual::add);

      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testReplaceKeys() throws IOException {
    Map<String, String> actualMap1 = new HashMap<>();
    actualMap1.put("map1test", "map1testval");

    Map<String, String> actualMap2 = new HashMap<>();
    actualMap2.put("map2test", "map2testval");
    actualMap2.put("map2_test_key", "map2testval2");

    Map<String, String> actualMap3Inner = new HashMap<>();
    actualMap3Inner.put("map3innertest", "map3innertestval");
    actualMap3Inner.put("map3_inner_test_key", "map3innertestval2");

    Map<String, Object> actualMap3 = new HashMap<>();
    actualMap3.put("map3test", "map3testval");
    actualMap3.put("map3_test_key", actualMap3Inner);

    Map<String, Object> actual = new HashMap<>();
    actual.put("test", "1");
    actual.put("test_key", "1");
    actual.put("map1", actualMap1);
    actual.put("map2", actualMap2);
    actual.put("map3_key", actualMap3);

    Map<String, String> expectedMap1 = new HashMap<>();
    expectedMap1.put("map1test", "map1testval");

    Map<String, String> expectedMap2 = new HashMap<>();
    expectedMap2.put("map2test", "map2testval");
    expectedMap2.put("map2TestKey", "map2testval2");

    Map<String, String> expectedMap3Inner = new HashMap<>();
    expectedMap3Inner.put("map3innertest", "map3innertestval");
    expectedMap3Inner.put("map3InnerTestKey", "map3innertestval2");

    Map<String, Object> expectedMap3 = new HashMap<>();
    expectedMap3.put("map3test", "map3testval");
    expectedMap3.put("map3TestKey", expectedMap3Inner);

    Map<String, Object> expected = new HashMap<>();
    expected.put("test", "1");
    expected.put("testKey", "1");
    expected.put("map1", expectedMap1);
    expected.put("map2", expectedMap2);
    expected.put("map3Key", expectedMap3);

    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("test", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("testKey", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("map1", Schema.nullableOf(Schema.recordOf(
        "map1",
        Schema.Field.of("map1test", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      ))),
      Schema.Field.of("map2", Schema.nullableOf(Schema.recordOf(
        "map2",
        Schema.Field.of("map2test", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("map2TestKey", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      ))),
      Schema.Field.of("map3Key", Schema.nullableOf(Schema.recordOf(
        "map3Key",
        Schema.Field.of("map3test", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("map3TestKey", Schema.nullableOf(Schema.recordOf(
          "map3TestKey",
          Schema.Field.of("map3innertest", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("map3InnerTestKey", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
        )))
      )))
    );

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      pagedIterator.replaceKeys(actual, schema);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testIsRecord() throws IOException {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      boolean actual = pagedIterator.isRecord(expected);
      Assert.assertTrue(actual);
    }
  }

  @Test
  public void testIsRecordInsideUnion() throws IOException {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      boolean actual = pagedIterator.isRecord(Schema.nullableOf(expected));
      Assert.assertTrue(actual);
    }
  }

  @Test
  public void testIsRecordUnionSimple() throws IOException {
    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      boolean actual = pagedIterator.isRecord(Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      Assert.assertFalse(actual);
    }
  }

  @Test
  public void testIsRecordSchemaSimple() throws IOException {
    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      boolean actual = pagedIterator.isRecord(Schema.of(Schema.Type.LONG));
      Assert.assertFalse(actual);
    }
  }

  @Test
  public void testGetRecordSchema() throws IOException {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Schema actual = pagedIterator.getRecordSchema(expected);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetRecordSchemaInsideUnion() throws IOException {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Schema actual = pagedIterator.getRecordSchema(Schema.nullableOf(expected));
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetRecordSchemaUnionSimple() throws IOException {
    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Schema actual = pagedIterator.getRecordSchema(Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testGetRecordSchemaSimple() throws IOException {
    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain)) {
      Schema actual = pagedIterator.getRecordSchema(Schema.of(Schema.Type.LONG));
      Assert.assertNull(actual);
    }
  }

  @Test
  public void testHasNext() throws IOException {
    ImmutableMap<String, Object> response = ImmutableMap.of(
      "groups", Arrays.asList(
        new HashMap<>(ImmutableMap.of("key", "val1")),
        new HashMap<>(ImmutableMap.of("key", "val2"))));

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain) {
      @Override
      Map<String, Object> getResponseAsMap() {
        return response;
      }
    }) {
      boolean actual = pagedIterator.hasNext();
      Assert.assertTrue(actual);

      String next = pagedIterator.next();
      Assert.assertEquals("{\"key\":\"val1\",\"object\":\"Groups\"}", next);
    }
  }

  @Test
  public void testHasNextPagination() throws IOException {
    Map<String, Object> response = new HashMap<>();
    response.put("groups", Collections.singletonList(
      new HashMap<>(ImmutableMap.of("key", "val1"))));
    response.put("next_page", "some_page");

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain) {
      @Override
      Map<String, Object> getResponseAsMap() {
        return response;
      }
    }) {
      boolean actual = pagedIterator.hasNext();
      Assert.assertTrue(actual);

      String next = pagedIterator.next();
      Assert.assertEquals("{\"key\":\"val1\",\"object\":\"Groups\"}", next);

      response.put("groups", Collections.singletonList(
        new HashMap<>(ImmutableMap.of("key", "val2"))));
      response.put("next_page", "null");

      actual = pagedIterator.hasNext();
      Assert.assertTrue(actual);

      next = pagedIterator.next();
      Assert.assertEquals("{\"key\":\"val2\",\"object\":\"Groups\"}", next);

      actual = pagedIterator.hasNext();
      Assert.assertFalse(actual);
    }
  }

  @Test
  public void testHasNextCurrentEmptyNextPageNull() throws IOException {
    ImmutableMap<String, Object> response = ImmutableMap.of(
      "groups", Collections.singletonList(
        new HashMap<>(ImmutableMap.of("key", "val1"))));

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain) {
      @Override
      Map<String, Object> getResponseAsMap() {
        return response;
      }
    }) {
      boolean actual = pagedIterator.hasNext();
      Assert.assertTrue(actual);

      String next = pagedIterator.next();
      Assert.assertEquals("{\"key\":\"val1\",\"object\":\"Groups\"}", next);

      actual = pagedIterator.hasNext();
      Assert.assertFalse(actual);
    }
  }

  @Test
  public void testHasNextCurrentEmptyNextPageNullString() throws IOException {
    ImmutableMap<String, Object> response = ImmutableMap.of(
      "groups", Collections.singletonList(
        new HashMap<>(ImmutableMap.of("key", "val1"))),
      "next_page", "null");

    ObjectType objectType = ObjectType.GROUPS;
    String subdomain = "subdomain";
    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      "Groups",
      "",
      "2019-01-01T23:01:01Z",
      "2019-01-01T23:01:01Z",
      "satisfactionRatingsScore",
      20,
      240,
      100,
      300,
      300,
      "http://%s.localhosttestdomain/%s",
      "");

    try (PagedIterator pagedIterator = new PagedIterator(config, objectType, subdomain) {
      @Override
      Map<String, Object> getResponseAsMap() {
        return response;
      }
    }) {
      boolean actual = pagedIterator.hasNext();
      Assert.assertTrue(actual);

      String next = pagedIterator.next();
      Assert.assertEquals("{\"key\":\"val1\",\"object\":\"Groups\"}", next);

      actual = pagedIterator.hasNext();
      Assert.assertFalse(actual);
    }
  }
}
