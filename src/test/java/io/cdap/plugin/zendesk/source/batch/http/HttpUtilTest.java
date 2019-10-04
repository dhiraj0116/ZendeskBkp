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

import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import io.cdap.plugin.zendesk.source.common.ObjectType;
import org.junit.Assert;
import org.junit.Test;

public class HttpUtilTest {

  @Test
  public void createFirstPageUrl() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.GROUPS;
    String expected = "https://test.zendesk.com/api/v2/groups.json";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlComments() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.ARTICLE_COMMENTS;
    Long entityId = 2L;
    String expected = "https://test.zendesk.com/api/v2/help_center/users/2/comments.json";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, entityId);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlBatch() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.ORGANIZATIONS;
    String startDate = "2019-01-01T23:01:01Z";
    String expected = "https://test.zendesk.com/api/v2/incremental/organizations.json?start_time=1546383661";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlBatchTicketComments() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.TICKET_COMMENTS;
    String startDate = "2019-01-01T23:01:01Z";
    String expected = "https://test.zendesk.com/api/v2/incremental" +
      "/ticket_events.json?include=comment_events&start_time=1546383661";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatings() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String expected = "https://test.zendesk.com/api/v2/satisfaction_ratings.json";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsStartDate() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String startDate = "2019-01-01T23:01:01Z";
    String expected = "https://test.zendesk.com/api/v2/satisfaction_ratings.json?start_time=1546383661";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      "",
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsEndDate() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String endDate = "2019-01-01T23:01:01Z";
    String expected = "https://test.zendesk.com/api/v2/satisfaction_ratings.json?end_time=1546383661";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      endDate,
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsScore() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String score = "Good";
    String expected = "https://test.zendesk.com/api/v2/satisfaction_ratings.json?score=good";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      "",
      score,
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsStartDateEndDate() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String startDate = "2019-01-01T23:01:01Z";
    String endDate = "2019-01-01T23:01:01Z";
    String expected = "https://test.zendesk.com/api/v2" +
      "/satisfaction_ratings.json?start_time=1546383661&end_time=1546383661";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      endDate,
      "",
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsStartDateScore() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String startDate = "2019-01-01T23:01:01Z";
    String score = "Good";
    String expected = "https://test.zendesk.com/api/v2" +
      "/satisfaction_ratings.json?start_time=1546383661&score=good";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      "",
      score,
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsEndDateScore() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String endDate = "2019-01-01T23:01:01Z";
    String score = "Good";
    String expected = "https://test.zendesk.com/api/v2" +
      "/satisfaction_ratings.json?end_time=1546383661&score=good";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      "",
      endDate,
      score,
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void createFirstPageUrlSatisfactionRatingsStartDateEndDateScore() {
    String zendeskBaseUrl = "https://%s.zendesk.com/api/v2/%s";
    String subdomain = "test";
    ObjectType objectType = ObjectType.SATISFACTION_RATINGS;
    String startDate = "2019-01-01T23:01:01Z";
    String endDate = "2019-01-01T23:01:01Z";
    String score = "Good";
    String expected = "https://test.zendesk.com/api/v2" +
      "/satisfaction_ratings.json?start_time=1546383661&end_time=1546383661&score=good";

    ZendeskBatchSourceConfig config = new ZendeskBatchSourceConfig(
      "reference",
      "email@test.com",
      "apiToken",
      subdomain,
      objectType.getObjectName(),
      "",
      startDate,
      endDate,
      score,
      20,
      240,
      100,
      300,
      300,
      zendeskBaseUrl,
      "");

    String actual = HttpUtil.createFirstPageUrl(config, objectType, subdomain, null);
    Assert.assertEquals(expected, actual);
  }
}
