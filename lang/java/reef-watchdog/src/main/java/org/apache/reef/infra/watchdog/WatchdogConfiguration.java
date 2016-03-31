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
package org.apache.reef.infra.watchdog;

import org.apache.reef.infra.watchdog.parameters.MaxNumEvaluatorsPerJob;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerAddress;
import org.apache.reef.infra.watchdog.parameters.WatchdogServerPort;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Created by anchung on 3/29/2016.
 */
public final class WatchdogConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> WATCHDOG_ADDRESS = new RequiredParameter<>();

  public static final RequiredParameter<Integer> WATCHDOG_PORT = new RequiredParameter<>();

  public static final RequiredParameter<Integer> MAX_NUM_EVALUATORS_PER_JOB = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new WatchdogConfiguration()
      .bindNamedParameter(WatchdogServerAddress.class, WATCHDOG_ADDRESS)
      .bindNamedParameter(WatchdogServerPort.class, WATCHDOG_PORT)
      .bindNamedParameter(MaxNumEvaluatorsPerJob.class, MAX_NUM_EVALUATORS_PER_JOB)
      .build();
}
