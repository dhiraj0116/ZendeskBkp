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
Feature: Zendesk Multi Source - Design time scenarios

  @TS-ZD-MULTI-DSGN-01
  Scenario: Verify user should be able to validate the plugin for Multiple objects
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
      | Groups | Organizations | Tickets |
    And fill Objects to skip List with below listed objects:
      | Ticket Comments |
    And Enter input plugin property: "startDate" with value: "start.date"
    And Enter input plugin property: "endDate" with value: "end.date"
    Then Validate "ZendeskMultiObjects" plugin properties
