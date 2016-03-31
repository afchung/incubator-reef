/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.infra.watchdog.server;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by anchung on 3/31/2016.
 */
public final class JobContainer {
  private final ConcurrentMap<String, Job> jobMap = new ConcurrentHashMap<>();

  @Inject
  private JobContainer() {
  }

  public boolean addJob(final String id, final Job job) {
    return jobMap.putIfAbsent(id, job) != null;
  }

  public void removeJob(final String id) {
    jobMap.remove(id);
  }

  public Job getJob(final String id) {
    return jobMap.get(id);
  }
}
