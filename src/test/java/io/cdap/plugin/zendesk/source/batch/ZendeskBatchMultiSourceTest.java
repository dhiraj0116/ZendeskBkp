/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.zendesk.source.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.junit.Assert;
import org.junit.Test;
import org.zendesk.client.v2.model.Group;
import org.zendesk.client.v2.model.User;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 *
 * Tests to verify configuration of {@link ZendeskBatchMultiSource}
 */
public class ZendeskBatchMultiSourceTest extends BaseZendeskBatchSourceTest {

  @Test
  public void testBatchMultiSource() throws Exception {
    User user = createUser();
    Group group = createGroup();

    ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("adminEmail", ADMIN_EMAIL)
      .put("apiToken", API_TOKEN)
      .put("subdomains", SUBDOMAIN)
      .put("objectsToPull", "Users,Groups")
      .put("startDate", ZonedDateTime.now().minusMinutes(10)
        .format(DateTimeFormatter.ISO_DATE_TIME))
      .put("maxRetryCount", "1")
      .put("maxRetryWait", "1")
      .put("maxRetryJitterWait", "1")
      .put("connectTimeout", "300")
      .put("readTimeout", "300")
      .put("zendeskBaseUrl", "https://%s.zendesk.com/api/v2/%s")
      .build();

    List<StructuredRecord> outputRecords = getPipelineResults(
      properties, ZendeskBatchMultiSource.NAME, "ZendeskBatchMulti");
    List<StructuredRecord> result = outputRecords.stream()
      .filter(record -> user.getId().equals(record.get("id")) || group.getId().equals(record.get("id")))
      .sorted(Comparator.comparing(r -> ((String) r.get("object"))))
      .collect(Collectors.toList());

    Assert.assertEquals(2, result.size());

    Assert.assertEquals(group.getId(), result.get(0).get("id"));
    Assert.assertEquals(group.getName(), result.get(0).get("name"));
    Assert.assertEquals("Groups", result.get(0).get("object"));

    Assert.assertEquals(user.getId(), result.get(1).get("id"));
    Assert.assertEquals(user.getName(), result.get(1).get("name"));
    Assert.assertEquals(user.getEmail(), result.get(1).get("email"));
    Assert.assertEquals("Users", result.get(1).get("object"));
  }
}
