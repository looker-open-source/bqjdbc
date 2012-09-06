/*
 * Copyright (c) 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package net.starschema.clouddb.cmdlineverification;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * Runs a Jetty server on a free port, waiting for OAuth to redirect to it with
 * the verification code.
 * <p>
 * Mostly copied from oacurl by phopkins@google.com.
 * </p>
 * 
 * @author Yaniv Inbar
 */
public final class LocalServerReceiver implements VerificationCodeReceiver {

    /**
     * Jetty handler that takes the verifier token passed over from the OAuth
     * provider and stashes it where {@link #waitForCode} will find it.
     */
    class CallbackHandler extends AbstractHandler {

        @Override
        public void handle(String target, HttpServletRequest request,
                HttpServletResponse response, int dispatch) throws IOException {
            if (!LocalServerReceiver.CALLBACK_PATH.equals(target))
                return;
            this.writeLandingHtml(response);
            response.flushBuffer();
            ((Request) request).setHandled(true);
            String error = request.getParameter("error");
            if (error != null) {
                System.out.println("Authorization failed. Error=" + error);
                System.out.println("Quitting.");
                System.exit(1);
            }
            LocalServerReceiver.this.code = request.getParameter("code");
            synchronized (LocalServerReceiver.this) {
                LocalServerReceiver.this.notify();
            }
        }

        private void writeLandingHtml(HttpServletResponse response)
                throws IOException {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html");

            PrintWriter doc = response.getWriter();
            doc.println("<html>");
            doc.println("<head><title>OAuth 2.0 Authentication Token Recieved</title></head>");
            doc.println("<body>");
            doc.println("Received verification code. Closing...");
            doc.println("<script type='text/javascript'>");
            // We open "" in the same window to trigger JS ownership of it,
            // which lets
            // us then close it via JS, at least in Chrome.
            doc.println("window.setTimeout(function() {");
            doc.println("    window.open('', '_self', ''); window.close(); }, 1000);");
            doc.println("if (window.opener) { window.opener.checkToken(); }");
            doc.println("</script>");
            doc.println("</body>");
            doc.println("</HTML>");
            doc.flush();
        }
    }

    private static final String CALLBACK_PATH = "/Callback";

    private static int getUnusedPort() throws IOException {
        Socket s = new Socket();
        s.bind(null);
        try {
            return s.getLocalPort();
        } finally {
            s.close();
        }
    }

    /** Server or {@code null} before {@link #getRedirectUri()}. */
    private Server server;

    /** Verification code or {@code null} before received. */
    volatile String code;

    @Override
    public String getRedirectUri() throws Exception {
        int port = LocalServerReceiver.getUnusedPort();
        this.server = new Server(port);
        for (Connector c : this.server.getConnectors())
            c.setHost("localhost");
        this.server.addHandler(new CallbackHandler());
        this.server.start();
        return "http://localhost:" + port + LocalServerReceiver.CALLBACK_PATH;

    }

    @Override
    public void stop() throws Exception {
        if (this.server != null) {
            this.server.stop();
            this.server = null;
        }
    }

    @Override
    public synchronized String waitForCode() {
        try {
            this.wait();
        } catch (InterruptedException exception) {
            // should not happen
        }
        return this.code;
    }
}
