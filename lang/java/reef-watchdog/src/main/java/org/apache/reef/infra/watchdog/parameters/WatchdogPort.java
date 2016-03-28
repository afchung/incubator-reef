package org.apache.reef.infra.watchdog.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by anchung on 3/28/2016.
 */
@NamedParameter(doc = "The port used for the Watchdog.")
public final class WatchdogPort implements Name<Integer> {
  private WatchdogPort(){
  }
}
