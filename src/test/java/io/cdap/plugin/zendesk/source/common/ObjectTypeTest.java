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

package io.cdap.plugin.zendesk.source.common;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.zendesk.source.common.config.BaseZendeskSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ObjectTypeTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testFromString() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    ObjectType.fromString("Groups", collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void testFromStringInvalidObject() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    try {
      ObjectType.fromString("Test", collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, collector.getValidationFailures().size());
      List<ValidationFailure.Cause> causeList = collector.getValidationFailures().get(0).getCauses();
      Assert.assertEquals(1, causeList.size());
      Assert.assertEquals(BaseZendeskSourceConfig.PROPERTY_OBJECTS_TO_PULL, collector.getValidationFailures().get(0)
        .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }
}
