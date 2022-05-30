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
Feature: Zendesk Multi Source - Design time validation scenarios

  @TS-ZD-MULTI-DSGN-ERROR-01
  Scenario: Verify required fields missing validation messages
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName   |
      | adminEmail      |
      | apiToken        |
      | subdomains      |

  @TS-ZD-MULTI-DSGN-ERROR-02
  Scenario: Verify validation message for Start date and End date in invalid format
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
      | Tickets |
    And Enter input plugin property: "startDate" with value: "invalid.start.date"
    And Enter input plugin property: "endDate" with value: "invalid.end.date"
    And Click on the Validate button
    Then Verify that the Plugin Property: "startDate" is displaying an in-line error message: "invalid.property.startdate"
    And Verify that the Plugin Property: "endDate" is displaying an in-line error message: "invalid.property.enddate"

  @TS-ZD-MULTI-DSGN-ERROR-03
  Scenario: Verify validation message for invalid subdomain
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Data Pipeline - Batch"
    And Select plugin: "Zendesk Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ZendeskMultiObjects"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Enter input plugin property: "adminEmail" with value: "admin.email" for Credentials and Authorization related fields
    And Enter input plugin property: "apiToken" with value: "admin.apitoken" for Credentials and Authorization related fields
    And configure Zendesk source plugin for listed subdomains:
      | INVALID_SUBDOMAIN |
    And fill Objects to pull List with below listed Objects:
      | Users |
    And Click on the Validate button
    Then Verify that the Plugin Property: "subdomains" is displaying an in-line error message: "invalid.property.subdomain"

