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

  @TS-ZD-RNTM-MACRO-01 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for a Non hierarchical Object with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "objectstopull.group" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-MACRO-02 @BQ_SINK
  Scenario: Verify user should be able to deploy and run the pipeline when Zendesk source is configured for a Non hierarchical Object with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "objectstopull.group" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @TS-ZD-RNTM-MACRO-03 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for Advanced Properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Requests Comments"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "maxRetryWait" and set the value to: "maxRetryWait"
    And Click on the Macro button of Property: "maxRetryJitterWait" and set the value to: "maxRetryJitterWait"
    And Click on the Macro button of Property: "connectTimeout" and set the value to: "connectTimeout"
    And Click on the Macro button of Property: "readTimeout" and set the value to: "readTimeout"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "zendesk.maxretrywait" for key "maxRetryWait"
    And Enter runtime argument value "zendesk.maxretryjitterwait" for key "maxRetryJitterWait"
    And Enter runtime argument value "zendesk.connecttimeout" for key "connectTimeout"
    And Enter runtime argument value "zendesk.readtimeout" for key "readTimeout"
    And Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-MACRO-04 @BQ_SINK
  Scenario: Verify user should be able to deploy and run the pipeline when Zendesk source is configured for Advanced Properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken"
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "Requests Comments"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "maxRetryWait" and set the value to: "maxRetryWait"
    And Click on the Macro button of Property: "maxRetryJitterWait" and set the value to: "maxRetryJitterWait"
    And Click on the Macro button of Property: "connectTimeout" and set the value to: "connectTimeout"
    And Click on the Macro button of Property: "readTimeout" and set the value to: "readTimeout"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "zendesk.maxretrywait" for key "maxRetryWait"
    And Enter runtime argument value "zendesk.maxretryjitterwait" for key "maxRetryJitterWait"
    And Enter runtime argument value "zendesk.connecttimeout" for key "connectTimeout"
    And Enter runtime argument value "zendesk.readtimeout" for key "readTimeout"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @TS-ZD-RNTM-MACRO-05 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk source is configured for a hierarchical Object with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "hierarchical.objectstopull.organization" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"

  @TS-ZD-RNTM-MACRO-06 @BQ_SINK
  Scenario: Verify user should be able to deploy and run the pipeline when Zendesk source is configured for a hierarchical Object with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Zendesk" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "hierarchical.objectstopull.organization" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @TS-ZD-RNTM-MACRO-07 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Credentials with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQuery" from the plugins list
    And Connect plugins: "Zendesk" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "admin.invalid.email" for key "adminEmail"
    And Enter runtime argument value "admin.invalid.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "objectstopull.group" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.credentials.logsmessage           |

  @TS-ZD-RNTM-MACRO-08 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Object with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQuery" from the plugins list
    And Connect plugins: "Zendesk" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "admin.invalid.object" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "start.date" for key "startDate"
    And Enter runtime argument value "end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.objecttopull.logsmessage          |

  @TS-ZD-RNTM-MACRO-09 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid start date and end date with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Click on the Macro button of Property: "adminEmail" and set the value to: "adminEmail"
    And Click on the Macro button of Property: "apiToken" and set the value to: "apiToken"
    And Click on the Macro button of Property: "subdomains" and set the value to: "subdomains"
    And Click on the Macro button of Property: "maxRetryCount" and set the value to: "maxRetryCount"
    And Click on the Macro button of Property: "objectsToPull" and set the value to: "objectsToPull"
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And Validate "Zendesk" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQuery" from the plugins list
    And Connect plugins: "Zendesk" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.email" for key "adminEmail"
    And Enter runtime argument value from environment variable "admin.apitoken" for key "apiToken"
    And Enter runtime argument value "admin.subdomain" for key "subdomains"
    And Enter runtime argument value "admin.invalid.object" for key "objectsToPull"
    And Enter runtime argument value "zendesk.maxretrycount" for key "maxRetryCount"
    And Enter runtime argument value "invalid.start.date" for key "startDate"
    And Enter runtime argument value "invalid.end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.date.logsmessage                  |
