package org.apache.reef.infra.watchdog;

import org.apache.reef.infra.watchdog.parameters.WatchdogPort;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.local.client.parameters.MaxNumberOfEvaluators;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.mortbay.jetty.Server;

import javax.inject.Inject;

/**
 * Created by anchung on 3/28/2016.
 */
public final class Launcher {

  private int maxNumEvaluatorsPerJob;
  private int watchdogPort;
  private WatchdogRequestHandler requestHandler;

  @Inject
  private Launcher(@Parameter(MaxNumberOfEvaluators.class) int maxNumEvaluatorsPerJob,
                   @Parameter(WatchdogPort.class) int watchdogPort,
                   WatchdogRequestHandler requestHandler) {
    this.maxNumEvaluatorsPerJob = maxNumEvaluatorsPerJob;
    this.watchdogPort = watchdogPort;
  }

  void run() throws Exception {
    Server server = new Server(watchdogPort);
    server.setHandler(requestHandler);
    server.start();
    server.join();
  }

  private Configuration getRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, maxNumEvaluatorsPerJob)
        .build();
  }

  public static void main(final String[] args) {
    if (args.length != 1) {
      throw new RuntimeException("The Launcher must have at least 1 argument containing the path to the " +
          "Launcher configuration file.");
    }

    final int port = Integer.parseInt(args[0]);
    final int numEvaluatorsPerJob = Integer.parseInt(args[1]);

    final Injector injector = Tang.Factory.getTang().newInjector();

    try {
      final Launcher launcher = injector.getInstance(Launcher.class);
      launcher.run();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
