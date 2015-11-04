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
package org.apache.reef.runtime.common.evaluator;

import org.apache.reef.proto.EvaluatorRuntimeProtocol;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.evaluator.context.ContextManager;
import org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import org.apache.reef.runtime.common.evaluator.parameters.HeartbeatPeriod;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Heartbeat manager.
 */
@Unit
public final class HeartBeatManager {

  private static final Logger LOG = Logger.getLogger(HeartBeatManager.class.getName());
  private static final int MaxHeartbeatFailures = 3;

  private final Object heartbeatLock = new Object();
  private final Clock clock;
  private final int heartbeatPeriod;
  private final InjectionFuture<EvaluatorRuntime> evaluatorRuntime;
  private final InjectionFuture<ContextManager> contextManager;
  private final Deque<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> heartbeatQueue = new LinkedList<>();

  private boolean recovery = false;
  private int heartbeatFailures = 0;
  private EventHandler<EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto> evaluatorHeartbeatHandler;

  @Inject
  private HeartBeatManager(
      final InjectionFuture<EvaluatorRuntime> evaluatorRuntime,
      final InjectionFuture<ContextManager> contextManager,
      final Clock clock,
      final RemoteManager remoteManager,
      @Parameter(HeartbeatPeriod.class) final int heartbeatPeriod,
      @Parameter(DriverRemoteIdentifier.class) final String driverRID) {
    this.evaluatorRuntime = evaluatorRuntime;
    this.contextManager = contextManager;
    this.clock = clock;
    this.heartbeatPeriod = heartbeatPeriod;
    this.evaluatorHeartbeatHandler = remoteManager.getHandler(
        driverRID, EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.class);
  }

  /**
   * Assemble a complete new heartbeat and send it out.
   */
  public void sendHeartbeat() {
    synchronized (heartbeatLock) {
      this.sendAndDrainHeartbeats(this.getEvaluatorHeartbeatProto());
    }
  }

  /**
   * Called with a specific TaskStatus that must be delivered to the driver.
   */
  public void sendTaskStatus(final ReefServiceProtos.TaskStatusProto taskStatusProto) {
    synchronized (heartbeatLock) {
      this.sendAndDrainHeartbeats(this.getEvaluatorHeartbeatProto(
          this.evaluatorRuntime.get().getEvaluatorStatus(),
          this.contextManager.get().getContextStatusCollection(),
          Optional.of(taskStatusProto)));
    }
  }

  /**
   * Called with a specific ContextStatus that must be delivered to the driver.
   */
  public void sendContextStatus(
      final ReefServiceProtos.ContextStatusProto contextStatusProto) {

    synchronized (heartbeatLock) {
      // TODO[JIRA REEF-833]: Write a test that verifies correct order of heartbeats.
      final Collection<ReefServiceProtos.ContextStatusProto> contextStatusList = new ArrayList<>();
      contextStatusList.add(contextStatusProto);
      contextStatusList.addAll(this.contextManager.get().getContextStatusCollection());

      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto =
          this.getEvaluatorHeartbeatProto(
              this.evaluatorRuntime.get().getEvaluatorStatus(),
              contextStatusList, Optional.<ReefServiceProtos.TaskStatusProto>empty());

      this.sendAndDrainHeartbeats(heartbeatProto);
    }
  }

  /**
   * Called with a specific EvaluatorStatus that must be delivered to the driver.
   */
  public void sendEvaluatorStatus(
      final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto) {
    synchronized (heartbeatLock) {
      this.sendAndDrainHeartbeats(EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.newBuilder()
          .setTimestamp(System.currentTimeMillis())
          .setEvaluatorStatus(evaluatorStatusProto)
          .build());
    }
  }

  /**
   * Sends the actual heartbeat out and logs it, if so desired.
   *
   * @param heartbeatProto
   */
  private void sendAndDrainHeartbeats(
      final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto heartbeatProto) {
    synchronized (heartbeatLock) {
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.FINEST, "Heartbeat message:\n" + heartbeatProto, new Exception("Stack trace"));
      }

      heartbeatQueue.addFirst(heartbeatProto);

      try {
        while (!heartbeatQueue.isEmpty()) {
          this.evaluatorHeartbeatHandler.onNext(heartbeatQueue.peekFirst());
          heartbeatFailures = 0;
          heartbeatQueue.removeFirst(); // Remove only if success
        }
      } catch (final Exception e) {
        LOG.log(Level.WARNING, "Exception when sending heartbeat: " + e);
        heartbeatFailures++;
        if (heartbeatFailures > MaxHeartbeatFailures) {
          recovery = true;
        }
      }
    }
  }

  private EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto getEvaluatorHeartbeatProto() {
    return this.getEvaluatorHeartbeatProto(
        this.evaluatorRuntime.get().getEvaluatorStatus(),
        this.contextManager.get().getContextStatusCollection(),
        this.contextManager.get().getTaskStatus());
  }

  private EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto getEvaluatorHeartbeatProto(
      final ReefServiceProtos.EvaluatorStatusProto evaluatorStatusProto,
      final Iterable<ReefServiceProtos.ContextStatusProto> contextStatusProtos,
      final Optional<ReefServiceProtos.TaskStatusProto> taskStatusProto) {

    final EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.Builder builder =
        EvaluatorRuntimeProtocol.EvaluatorHeartbeatProto.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setEvaluatorStatus(evaluatorStatusProto);

    for (final ReefServiceProtos.ContextStatusProto contextStatusProto : contextStatusProtos) {
      builder.addContextStatus(contextStatusProto);
    }

    if (taskStatusProto.isPresent()) {
      builder.setTaskStatus(taskStatusProto.get());
    }

    return builder.build();
  }

  private void recover() {
    new DefaultDriverConnection();
  }

  final class HeartbeatAlarmHandler implements EventHandler<Alarm> {
    @Override
    public void onNext(final Alarm alarm) {
      synchronized (HeartBeatManager.this.heartbeatLock) {
        if (evaluatorRuntime.get().isRunning()) {
          if (recovery) {
            HeartBeatManager.this.recover();
          }

          HeartBeatManager.this.sendHeartbeat();
          HeartBeatManager.this.clock.scheduleAlarm(HeartBeatManager.this.heartbeatPeriod, this);
        } else {
          LOG.log(Level.FINEST,
              "Not triggering a heartbeat, because state is: {0}",
              evaluatorRuntime.get().getState());
        }
      }
    }
  }
}
