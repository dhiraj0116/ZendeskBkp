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

@ZendeskMultiSource
@Smoke
@Regression
Feature: Zendesk Multi Source - Run time scenarios

  @TS-ZD-MULTI-RNTM-01 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when Zendesk Multi Source is configured for Multiple objects
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Groups | Post Comments | Article Comments | Requests Comments |
    And fill Objects to skip List with below listed objects:
      | Users | Organizations |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    And Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Open and capture pipeline preview logs
    Then Verify the preview of pipeline is "success"

  @TS-ZD-MULTI-RNTM-02 @BQ_SINK
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk Multi Source is configured for Multiple objects
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Groups | Post Comments | Article Comments | Requests Comments |
    And fill Objects to skip List with below listed objects:
      | Users | Organizations |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    And Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    Then Verify the pipeline status is "Succeeded"

  @TS-ZD-MULTI-RNTM-03 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when a Single Object is configured for both Object to Pull and Object to Skip operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Ticket Comments | Tickets | Groups |
    And fill Objects to skip List with below listed objects:
      | Ticket Comments |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    And Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Open and capture pipeline preview logs
    Then Verify the preview of pipeline is "success"

  @TS-ZD-MULTI-RNTM-04 @BQ_SINK
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk MultiSource Single Object is configured for both Object to Pull and Object to Skip operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Ticket Comments | Tickets | Groups |
    And fill Objects to skip List with below listed objects:
      | Groups |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    And Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    Then Verify the pipeline status is "Succeeded"

  @TS-ZD-MULTI-RNTM-05
  Scenario: Verify user should be able to preview the pipeline when Zendesk MultiSource is configured for a Herarchical objects with File Sink
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Users | Tickets |
    And fill Objects to skip List with below listed objects:
      | Groups |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "File" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "File" to establish connection
    And Navigate to the properties page of plugin: "File"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "path" with value: "file.path"
    And Select dropdown plugin property: "format" with option value: "json"
    And Validate "File" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    Then Verify the preview of pipeline is "success"

  @TS-ZD-MULTI-RNTM-06
  Scenario: Verify user should be able to deploy and Run the pipeline when Zendesk MultiSource is configured for a Herarchical objects with File Sink
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And fill Objects to pull List with below listed Objects:
      | Users | Tickets |
    And fill Objects to skip List with below listed objects:
      | Groups |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Validate "ZendeskMultiObjects" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "File" from the plugins list
    And Connect source as "ZendeskMultiObjects" and sink as "File" to establish connection
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

