package org.apache.reef.infra.watchdog.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by anchung on 3/28/2016.
 */
@NamedParameter(doc = "The maximum number of Evaluators per job dispatched by the WatchDog.")
public final class MaxNumEvaluatorsPerJob implements Name<Integer> {
  private MaxNumEvaluatorsPerJob() {
  }
}
