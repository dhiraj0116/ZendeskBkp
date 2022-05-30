/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.zendesk.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.utils.enums.Subdomains;
import io.cdap.plugin.zendesk.locators.ZendeskPropertiesPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Zendesk batch source - Properties page - Actions.
 */
public class ZendeskPropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(ZendeskPropertiesPageActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(ZendeskPropertiesPage.class);

  }

  public static void configureSubdomains(List<String> subdomainList) {
    List<Subdomains> tables = new ArrayList<>();

    for(String table : subdomainList) {

      tables.add(Subdomains.valueOf(table));
    }
    fillsubdomainsInSubdomainSpecificationSection(tables);
  }

  public static void fillsubdomainsInSubdomainSpecificationSection(List<Subdomains> subdomainList) {
    for (int i = 0; i < subdomainList.size() - 1; i++) {
      ElementHelper.clickOnElement(ZendeskPropertiesPage.addRowButtonInSubdomainsField.get(i));
    }

    for (int i = 0; i < subdomainList.size(); i++) {
      ElementHelper.sendKeys(ZendeskPropertiesPage.subdomainsInputs.get(i), subdomainList.get(i).value);
    }
  }

  public static void selectDropdowWithMultipleOptionsForObjectsToPull(List<String> tablesList) {
    int objectsToPull = tablesList.size();

    ZendeskPropertiesPage.objectDropdownForMultiObjectsToPull.click();
    ZendeskPropertiesPage.selectOptionGroups.click();

    for (int i = 0; i < objectsToPull; i++) {
      logger.info("Select checkbox option: " + tablesList.get(i));
      ElementHelper.selectCheckbox(ZendeskPropertiesPage.
                                     locateObjectCheckBoxInMultiObjectsSelector(tablesList.get(i)));
    }

    ElementHelper.clickUsingActions(CdfPluginPropertiesLocators.pluginPropertiesPageHeader);
  }

  public static void selectDropdowWithMultipleOptionsForObjectsToSkip(List<String> tablesList) {
    int objectsToPull = tablesList.size();

    ZendeskPropertiesPage.objectDropdownForMultiObjectsToSkip.click();

    for (int i = 0; i < objectsToPull; i++) {
      logger.info("Select checkbox option: " + tablesList.get(i));
      ElementHelper.selectCheckbox(ZendeskPropertiesPage.
                                     locateObjectCheckBoxInMultiObjectsSelector(tablesList.get(i)));
    }
    ElementHelper.clickUsingActions(CdfPluginPropertiesLocators.pluginPropertiesPageHeader);
  }


}

