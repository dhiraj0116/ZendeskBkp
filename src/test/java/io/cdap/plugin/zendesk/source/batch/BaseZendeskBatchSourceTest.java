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

package io.cdap.plugin.zendesk.source.batch;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zendesk.client.v2.Zendesk;
import org.zendesk.client.v2.model.Group;
import org.zendesk.client.v2.model.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base test class that sets up plugin and the etl batch app artifacts.
 * <p>
 * By default all tests will be skipped, since Zendesk credentials are needed.
 * <p>
 * Instructions to enable the tests:
 * 1. Create/use existing Zendesk account.
 * 2. Create API Token.
 * 3. Run the tests using the command below:
 * <p>
 * mvn clean test
 * -Dzendesk.test.adminEmail= -Dzendesk.test.apiToken=
 * -Dzendesk.test.subdomain=
 */
public abstract class BaseZendeskBatchSourceTest extends HydratorTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BaseZendeskBatchSourceTest.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final String ADMIN_EMAIL = System.getProperty("zendesk.test.adminEmail");
  protected static final String API_TOKEN = System.getProperty("zendesk.test.apiToken");
  protected static final String SUBDOMAIN = System.getProperty("zendesk.test.subdomain");

  private static final List<Long> USER_IDS = new ArrayList<>();
  private static final List<Long> GROUP_IDS = new ArrayList<>();

  private static Zendesk zendeskClient;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setupTestClass() throws Exception {
    try {
      Assume.assumeNotNull(ADMIN_EMAIL, API_TOKEN, SUBDOMAIN);
    } catch (AssumptionViolatedException e) {
      LOG.warn("ETL tests are skipped. Please find the instructions on enabling it at " +
                 "BaseZendeskBatchSourceTest javadoc.");
      throw e;
    }

    zendeskClient = new Zendesk.Builder(String.format("https://%s.zendesk.com/", SUBDOMAIN))
      .setUsername(ADMIN_EMAIL)
      .setToken(API_TOKEN)
      .build();

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      ZendeskBatchSource.class);
  }

  @AfterClass
  public static void tearDownTestClass() {
    USER_IDS.forEach(BaseZendeskBatchSourceTest::deleteUser);
    GROUP_IDS.forEach(BaseZendeskBatchSourceTest::deleteGroup);
    if (zendeskClient != null) {
      zendeskClient.close();
    }
  }

  protected List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties,
                                                      String pluginName,
                                                      String applicationPrefix) throws Exception {
    ETLStage source = new ETLStage("ZendeskReader", new ETLPlugin(
      pluginName, BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(applicationPrefix + "_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }

  protected User createUser() {
    String methodName = testName.getMethodName();
    User user = zendeskClient.createUser(
      new User(true, "Test User " + methodName, methodName + "@domain.com"));
    USER_IDS.add(user.getId());
    return user;
  }

  private static void deleteUser(Long userId) {
    zendeskClient.deleteUser(userId);
  }

  protected Group createGroup() {
    String methodName = testName.getMethodName();
    Group group = new Group();
    group.setName("Test Group " + methodName);
    group = zendeskClient.createGroup(group);
    GROUP_IDS.add(group.getId());
    return group;
  }

  private static void deleteGroup(Long groupId) {
    zendeskClient.deleteGroup(groupId);
  }
}
