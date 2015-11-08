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
package org.apache.reef.bridge.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.launch.LauncherCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by anchung on 11/7/2015.
 */
@Private
public final class BootstrapLauncherCommand implements LauncherCommand {
  private final List<String> flags = new ArrayList<>();
  private String configFileName = "";

  private BootstrapLauncherCommand() {
  }

  public static BootstrapLauncherCommand getLauncherCommand(){
    return new BootstrapLauncherCommand();
  }

  @Override
  public LauncherCommand addFlag(final String parameter) {
    flags.add(parameter);
    return this;
  }

  @Override
  public String getLauncherClass() {
    return BootstrapLauncher.class.getName();
  }

  @Override
  public List<String> getFlags() {
    return Collections.unmodifiableList(flags);
  }

  @Override
  public LauncherCommand setConfigurationFileName(final String configurationFileName) {
    configFileName = configurationFileName;
    return this;
  }

  @Override
  public String getConfigurationFileName() {
    return configFileName;
  }
}
