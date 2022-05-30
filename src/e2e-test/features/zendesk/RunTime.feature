# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@ZendeskSource
@Smoke
@Regression
Feature: Zendesk Source - Run time scenarios

  @TS-ZD-RNTM-01 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for a Non hierarchical object
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Groups"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-02 @BQ_SINK
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk source is configured for a Non hierarchical object
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Groups"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"


  @TS-ZD-RNTM-03 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for a hierarchical object
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Ticket Comments"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-04 @BQ_SINK
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk source is configured for a hierarchical object
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Ticket Comments"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"

  @TS-ZD-RNTM-05 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Article Comments"
    And Replace input plugin property: "maxRetryCount" with value: "zendesk.maxretrycount"
    And Replace input plugin property: "maxRetryWait" with value: "zendesk.maxretrywait"
    And Replace input plugin property: "maxRetryJitterWait" with value: "zendesk.maxretryjitterwait"
    And Replace input plugin property: "connectTimeout" with value: "zendesk.connecttimeout"
    And Replace input plugin property: "readTimeout" with value: "zendesk.readtimeout"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-06 @BQ_SINK
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk source is configured for Advanced Properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Article Comments"
    And Replace input plugin property: "maxRetryCount" with value: "zendesk.maxretrycount"
    And Replace input plugin property: "maxRetryWait" with value: "zendesk.maxretrywait"
    And Replace input plugin property: "maxRetryJitterWait" with value: "zendesk.maxretryjitterwait"
    And Replace input plugin property: "connectTimeout" with value: "zendesk.connecttimeout"
    And Replace input plugin property: "readTimeout" with value: "zendesk.readtimeout"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Connect source as "Zendesk" and sink as "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"

  @TS-ZD-RNTM-07
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for a Herarchical object with File Sink
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Users"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "File" from the plugins list
    And Connect source as "Zendesk" and sink as "File" to establish connection
    And Navigate to the properties page of plugin: "File"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "path" with value: "file.path"
    And Select dropdown plugin property: "format" with option value: "json"
    And Validate "File" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-08
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk source is configured for a herarchical object with File Sink
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Ticket Comments"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "File" from the plugins list
    And Connect source as "Zendesk" and sink as "File" to establish connection
    And Navigate to the properties page of plugin: "File"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "path" with value: "file.path"
    And Select dropdown plugin property: "format" with option value: "json"
    And Validate "File" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"

