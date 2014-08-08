/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.tests.yarn.failure;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class FailureDriver {

  private static final int NUM_EVALUATORS = 40;
  private static final int NUM_FAILURES = 10;

  private static final Logger LOG = Logger.getLogger(FailureDriver.class.getName());

  private final EvaluatorRequestor requestor;

  private final AtomicInteger toSubmit = new AtomicInteger(NUM_FAILURES);

  @Inject
  public FailureDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    LOG.info("Driver instantiated");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.FINE, "Request {0} Evaluators.", NUM_EVALUATORS);
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(NUM_EVALUATORS)
          .setMemory(64)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit a poisoned context.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINE, "Got allocated evaluator: {0}", evalId);
      if (toSubmit.getAndDecrement() > 0) {
        LOG.log(Level.FINE, "Submitting poisoned context. {0} to go.", toSubmit);
        allocatedEvaluator.submitContext(
            Tang.Factory.getTang()
                .newConfigurationBuilder(
                    ContextConfiguration.CONF
                        .set(ContextConfiguration.IDENTIFIER, "Poisoned Context: " + evalId)
                        .build(),
                    PoisonedConfiguration.CONTEXT_CONF
                        .set(PoisonedConfiguration.CRASH_PROBABILITY, "1")
                        .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                        .build())
                .build());
      } else {
        LOG.log(Level.FINE, "Closing evaluator {0}", evalId);
        allocatedEvaluator.close();
      }
    }
  }

  /**
   * Handles FailedEvaluator: Resubmits the single Evaluator resource request.
   */
  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.FINE, "Got failed evaluator: {0} - re-request", failedEvaluator.getId());
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .build());
    }
  }
}
