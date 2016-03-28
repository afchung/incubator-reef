package org.apache.reef.infra.watchdog;

import org.mortbay.jetty.handler.ContextHandler;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by anchung on 3/28/2016.
 */
public class WatchdogRequestHandler extends ContextHandler {

  @Inject
  private WatchdogRequestHandler() {
    super.setContextPath("/submit");
  }

  @Override
  public void handle(final String requestTarget,
                     final HttpServletRequest httpServletRequest,
                     final HttpServletResponse httpServletResponse,
                     final int dispatchMode) throws IOException, ServletException {
  }
}
