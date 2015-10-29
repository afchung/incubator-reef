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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Private;

/**
 * Created by anchung on 10/29/2015.
 */
@Private
public final class BridgeHandlerManager {
  private long allocatedEvaluatorHandler = 0;
  private long activeContextHandler = 0;
  private long taskMessageHandler = 0;
  private long failedTaskHandler = 0;
  private long failedEvaluatorHandler = 0;
  private long httpServerEventHandler = 0;
  private long completedTaskHandler = 0;
  private long runningTaskHandler = 0;
  private long suspendedTaskHandler = 0;
  private long completedEvaluatorHandler = 0;
  private long closedContextHandler = 0;
  private long failedContextHandler = 0;
  private long contextMessageHandler = 0;
  private long driverRestartActiveContextHandler = 0;
  private long driverRestartRunningTaskHandler = 0;
  private long driverRestartCompletedHandler = 0;
  private long driverRestartFailedEvaluatorHandler = 0;

  public BridgeHandlerManager(final long allocatedEvaluatorHandler,
                              final long activeContextHandler,
                              final long taskMessageHandler,
                              final long failedTaskHandler,
                              final long failedEvaluatorHandler,
                              final long httpServerEventHandler,
                              final long completedTaskHandler,
                              final long runningTaskHandler,
                              final long suspendedTaskHandler,
                              final long completedEvaluatorHandler,
                              final long closedContextHandler,
                              final long failedContextHandler,
                              final long contextMessageHandler,
                              final long driverRestartActiveContextHandler,
                              final long driverRestartRunningTaskHandler,
                              final long driverRestartCompletedHandler,
                              final long driverRestartFailedEvaluatorHandler) {
    this.allocatedEvaluatorHandler = allocatedEvaluatorHandler;
    this.activeContextHandler = activeContextHandler;
    this.taskMessageHandler = taskMessageHandler;
    this.failedTaskHandler = failedTaskHandler;
    this.failedEvaluatorHandler = failedEvaluatorHandler;
    this.httpServerEventHandler = httpServerEventHandler;
    this.completedTaskHandler = completedTaskHandler;
    this.runningTaskHandler = runningTaskHandler;
    this.suspendedTaskHandler = suspendedTaskHandler;
    this.completedEvaluatorHandler = completedEvaluatorHandler;
    this.closedContextHandler = closedContextHandler;
    this.failedContextHandler = failedContextHandler;
    this.contextMessageHandler = contextMessageHandler;
    this.driverRestartActiveContextHandler = driverRestartActiveContextHandler;
    this.driverRestartRunningTaskHandler = driverRestartRunningTaskHandler;
    this.driverRestartCompletedHandler = driverRestartCompletedHandler;
    this.driverRestartFailedEvaluatorHandler = driverRestartFailedEvaluatorHandler;
  }

  public long getAllocatedEvaluatorHandler() {
    return allocatedEvaluatorHandler;
  }

  public long getActiveContextHandler() {
    return activeContextHandler;
  }

  public long getTaskMessageHandler() {
    return taskMessageHandler;
  }

  public long getFailedTaskHandler() {
    return failedTaskHandler;
  }

  public long getFailedEvaluatorHandler() {
    return failedEvaluatorHandler;
  }

  public long getHttpServerEventHandler() {
    return httpServerEventHandler;
  }

  public long getCompletedTaskHandler() {
    return completedTaskHandler;
  }

  public long getRunningTaskHandler() {
    return runningTaskHandler;
  }

  public long getSuspendedTaskHandler() {
    return suspendedTaskHandler;
  }

  public long getCompletedEvaluatorHandler() {
    return completedEvaluatorHandler;
  }

  public long getClosedContextHandler() {
    return closedContextHandler;
  }

  public long getFailedContextHandler() {
    return failedContextHandler;
  }

  public long getContextMessageHandler() {
    return contextMessageHandler;
  }

  public long getDriverRestartActiveContextHandler() {
    return driverRestartActiveContextHandler;
  }

  public long getDriverRestartRunningTaskHandler() {
    return driverRestartRunningTaskHandler;
  }

  public long getDriverRestartCompletedHandler() {
    return driverRestartCompletedHandler;
  }

  public long getDriverRestartFailedEvaluatorHandler() {
    return driverRestartFailedEvaluatorHandler;
  }
}
