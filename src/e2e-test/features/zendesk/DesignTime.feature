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
Feature: Zendesk Source - Design time scenarios

  @TS-ZD-DSGN-01
  Scenario Outline: Verify user should be able to validate the Output Schema for Objects without Start date
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
    | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "<ObjectsToPull>"
    And Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "<ExpectedSchema>"
    Examples:
      | ObjectsToPull      | ExpectedSchema                       |
      | Groups             | schema.objecttopull.groups           |
      | Article Comments   | schema.objecttopull.articlecomments  |
      | Post Comments      | schema.objecttopull.postcomments     |
      | Requests Comments  | schema.objecttopull.requestscomments |
      | Tags               | schema.objecttopull.tags             |
      | Ticket Fields      | schema.objecttopull.ticketfields     |
      | Ticket Metrics     | schema.objecttopull.ticketmetrics    |


  @TS-ZD-DSGN-02
  Scenario Outline: Verify user should be able to validate the Output Schema for Objects with Start date
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "<ObjectsToPull>"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "<ExpectedSchema>"
    Examples:
      | ObjectsToPull         | ExpectedSchema                       |
      | Ticket Comments       | schema.objecttopull.ticketcomments   |
      | Tickets               | schema.objecttopull.tickets          |
      | Users                 | schema.objecttopull.users            |
      | Organizations         | schema.objecttopull.organizations    |
      | Ticket Metric Events  | schema.ticketmetric.events           |

  @TS-ZD-DSGN-03
  Scenario Outline: Verify user should be able to validate the Output Schema for Objects with Start date and End date
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Zendesk"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | CLOUD_SUFI |
    And Select dropdown plugin property: "objectsToPull" with option value: "<ObjectsToPull>"
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    And Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "<ExpectedSchema>"
    Examples:
      | ObjectsToPull         | ExpectedSchema                         |
      | Satisfaction Ratings  | schema.objecttopull.satisfactionrating |


  @TS-ZD-DSGN-04
  Scenario: Verify user should be able to validate the Output Schema for Object which has Heirarchical data
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
    And Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema for listed Hierarchical fields:
      | FieldName   |         SchemaJsonArray              |
      | attachments | heirarchicalschema.articlecomments   |
