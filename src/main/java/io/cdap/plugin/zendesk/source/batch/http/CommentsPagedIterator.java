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

package io.cdap.plugin.zendesk.source.batch.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.zendesk.source.batch.ZendeskBatchSourceConfig;
import io.cdap.plugin.zendesk.source.common.ObjectType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterable wrapper for Zendesk comments
 */
public class CommentsPagedIterator implements Iterator<String>, Closeable {

  private static final Gson GSON = new GsonBuilder().create();

  private final PagedIterator entityIterator;
  private final ZendeskBatchSourceConfig config;
  private final ObjectType objectType;
  private final String subdomain;
  private PagedIterator pagedIterator;

  public CommentsPagedIterator(PagedIterator entityIterator,
                               ZendeskBatchSourceConfig config, ObjectType objectType, String subdomain) {
    this.entityIterator = entityIterator;
    this.config = config;
    this.objectType = objectType;
    this.subdomain = subdomain;
  }

  @Override
  public boolean hasNext() {
    if (pagedIterator == null || !pagedIterator.hasNext()) {
      if (!entityIterator.hasNext()) {
        return false;
      }
      String next = entityIterator.next();
      Map userMap = GSON.fromJson(next, Map.class);
      Long userId = ((Number) userMap.get("id")).longValue();
      pagedIterator = new PagedIterator(config, objectType, subdomain, userId);
    }
    return pagedIterator.hasNext();
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return pagedIterator.next();
  }

  @Override
  public void close() throws IOException {
    if (pagedIterator != null) {
      pagedIterator.close();
    }
  }
}
