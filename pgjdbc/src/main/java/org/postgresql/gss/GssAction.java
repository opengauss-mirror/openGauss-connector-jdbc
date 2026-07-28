/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.gss;

import org.postgresql.core.EncodingPredictor;
import org.postgresql.core.PGStream;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Builds the GSS security context and exchanges authentication tokens with the server.
 *
 * @since 2026-07-31
 */
class GssAction implements PrivilegedAction<GssAction.AuthenticationResult> {
    static final String KERBEROS_SERVER_HOSTNAME = "kerberosServerHostname";
    private static Log logger = Logger.getLogger(GssAction.class.getName());

    private final PGStream pgStream;
    private final String host;
    private final String user;
    private final String kerberosServerName;
    private final String kerberosServerHostname;
    private final boolean shouldUseSpnego;
    private final GSSCredential clientCredentials;
    private final String socketAddress;

    GssAction(PGStream pgStream, GSSCredential clientCredentials, String host, String user,
            String kerberosServerName, boolean shouldUseSpnego, String kerberosServerHostname) {
        this.pgStream = pgStream;
        this.clientCredentials = clientCredentials;
        this.host = host;
        this.user = user;
        this.kerberosServerName = kerberosServerName;
        this.kerberosServerHostname = kerberosServerHostname;
        this.shouldUseSpnego = shouldUseSpnego;
        this.socketAddress = pgStream.getConnectInfo();
    }

    private static boolean hasSpnegoSupport(GSSManager manager) throws GSSException {
        org.ietf.jgss.Oid spnego = new org.ietf.jgss.Oid("1.3.6.1.5.5.2");
        org.ietf.jgss.Oid[] mechs = manager.getMechs();

        for (Oid mech : mechs) {
            if (mech.equals(spnego)) {
                return true;
            }
        }

        return false;
    }

    static String getKerberosServerHostname(String kerberosServerHostname, String host) {
        if (kerberosServerHostname == null || kerberosServerHostname.length() == 0) {
            /*
             * Missing connection-level hostname falls back to the current connection
             * endpoint, not a mutable process-wide value left by another connection.
             */
            return host;
        }
        return kerberosServerHostname;
    }

    static final class AuthenticationResult {
        private final Exception exception;
        private final byte[] token;

        private AuthenticationResult(Exception exception, byte[] token) {
            this.exception = exception;
            this.token = token;
        }

        static AuthenticationResult success() {
            return new AuthenticationResult(null, null);
        }

        static AuthenticationResult failure(Exception exception) {
            return new AuthenticationResult(exception, null);
        }

        static AuthenticationResult token(byte[] token) {
            return new AuthenticationResult(null, token);
        }

        Exception getException() {
            return exception;
        }

        byte[] getToken() {
            return token;
        }
    }

    private GSSContext getSecContext() throws GSSException {
        GSSManager manager = GSSManager.getInstance();
        GSSCredential clientCreds = null;
        Oid[] desiredMechs = new Oid[1];
        if (clientCredentials == null) {
            if (shouldUseSpnego && hasSpnegoSupport(manager)) {
                desiredMechs[0] = new Oid("1.3.6.1.5.5.2");
            } else {
                desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
            }
            GSSName clientName = manager.createName(user, GSSName.NT_USER_NAME);
            clientCreds = manager.createCredential(clientName, 8 * 3600, desiredMechs,
                    GSSCredential.INITIATE_ONLY);
        } else {
            desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
            clientCreds = clientCredentials;
        }

        GSSName serverName =
                manager.createName(kerberosServerName + "@" + getKerberosServerHostname(
                        kerberosServerHostname, host), GSSName.NT_HOSTBASED_SERVICE);

        GSSContext secContext = manager.createContext(serverName, desiredMechs[0], clientCreds,
                GSSContext.DEFAULT_LIFETIME);
        return secContext;
    }

    /**
     * Runs GSS token exchange and returns the authentication result.
     *
     * @return authentication result with an exception when negotiation fails
     */
    @Override
    public AuthenticationResult run() {
        try {
            return exchangeAuthenticationTokens();
        } catch (IOException e) {
            logger.info("GSS authentication token exchange failed due to I/O error.", e);
            return AuthenticationResult.failure(e);
        } catch (GSSException gsse) {
            logger.info("GSS authentication token exchange failed due to GSS error.", gsse);
            PSQLException gssException = new PSQLException(GT.tr("GSS Authentication failed"),
                    PSQLState.CONNECTION_FAILURE, gsse);
            return AuthenticationResult.failure(gssException);
        }
    }

    private AuthenticationResult exchangeAuthenticationTokens()
            throws IOException, GSSException {
        GSSContext secContext = getSecContext();
        return exchangeAuthenticationTokens(secContext);
    }

    AuthenticationResult exchangeAuthenticationTokens(GSSContext secContext)
            throws IOException, GSSException {
        secContext.requestMutualAuth(true);

        byte[] inToken = new byte[0];
        while (!secContext.isEstablished()) {
            byte[] outToken = secContext.initSecContext(inToken, 0, inToken.length);
            sendAuthenticationToken(outToken);

            if (!secContext.isEstablished()) {
                AuthenticationResult readResult = receiveNextToken();
                if (readResult.getException() != null) {
                    return readResult;
                }
                inToken = readResult.getToken();
            }
        }

        return AuthenticationResult.success();
    }

    private void sendAuthenticationToken(byte[] outToken) throws IOException {
        if (outToken == null) {
            return;
        }

        logger.trace(" FE=> Password(GSS Authentication Token)");

        pgStream.sendChar('p');
        pgStream.sendInteger4(4 + outToken.length);
        pgStream.send(outToken);
        pgStream.flush();
    }

    private AuthenticationResult receiveNextToken() throws IOException {
        int response = pgStream.receiveChar();
        switch (response) {
            case 'E':
                return receiveAuthenticationError();
            case 'R':
                return receiveAuthenticationContinue();
            default:
                return protocolError();
        }
    }

    private AuthenticationResult receiveAuthenticationError() throws IOException {
        int errorLength = pgStream.receiveInteger4();
        EncodingPredictor.DecodeResult errorResult =
                pgStream.receiveErrorString(errorLength - 4);
        ServerErrorMessage errorMsg = new ServerErrorMessage(errorResult, socketAddress);

        if (logger.isTraceEnabled()) {
            logger.trace(" <=BE ErrorMessage(" + errorMsg + ")");
        }

        return AuthenticationResult.failure(new PSQLException(errorMsg));
    }

    private AuthenticationResult receiveAuthenticationContinue() throws IOException {
        logger.trace(" <=BE AuthenticationGSSContinue");
        int len = pgStream.receiveInteger4();
        // should check type = 8
        pgStream.receiveInteger4();
        return AuthenticationResult.token(pgStream.receive(len - 8));
    }

    private AuthenticationResult protocolError() {
        PSQLException protocolError = new PSQLException(
                GT.tr("Protocol error.  Session setup failed."),
                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
        return AuthenticationResult.failure(protocolError);
    }
}
