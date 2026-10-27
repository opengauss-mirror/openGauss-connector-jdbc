/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.gss;

import org.postgresql.core.PGStream;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import org.ietf.jgss.GSSCredential;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Performs GSS authentication and creates the GssAction used for token exchange.
 *
 * @since 2026-07-31
 */
public class MakeGSS {
    private static Log logger = Logger.getLogger(MakeGSS.class.getName());

    /**
     * Carries all inputs required by one GSS authentication attempt.
     *
     * @since 2026-07-31
     */
    public static class AuthenticationRequest {
        private final PGStream pgStream;
        private final String host;
        private final String user;
        private final String credential;
        private String jaasApplicationName;
        private String kerberosServerName;
        private String kerberosServerHostname;
        private boolean shouldUseSpnego;
        private boolean shouldPerformJaasLogin;

        /**
         * Creates a request with the required stream and user identity data.
         *
         * @param pgStream the PostgreSQL stream used for token exchange
         * @param host the current connection host
         * @param user the user being authenticated
         * @param credential the secret used by the JAAS callback handler
         */
        public AuthenticationRequest(PGStream pgStream, String host, String user,
                String credential) {
            this.pgStream = pgStream;
            this.host = host;
            this.user = user;
            this.credential = credential;
        }

        /**
         * Sets the JAAS application name.
         *
         * @param value the JAAS application name
         * @return this request
         */
        public AuthenticationRequest jaasApplicationName(String value) {
            this.jaasApplicationName = value;
            return this;
        }

        /**
         * Sets the Kerberos service name.
         *
         * @param value the Kerberos service name
         * @return this request
         */
        public AuthenticationRequest kerberosServerName(String value) {
            this.kerberosServerName = value;
            return this;
        }

        /**
         * Sets whether SPNEGO should be preferred when available.
         *
         * @param shouldUseSpnego true when SPNEGO should be preferred
         * @return this request
         */
        public AuthenticationRequest shouldUseSpnego(boolean shouldUseSpnego) {
            this.shouldUseSpnego = shouldUseSpnego;
            return this;
        }

        /**
         * Sets whether this method should perform JAAS login.
         *
         * @param shouldPerformJaasLogin true when JAAS login should be performed
         * @return this request
         */
        public AuthenticationRequest shouldPerformJaasLogin(boolean shouldPerformJaasLogin) {
            this.shouldPerformJaasLogin = shouldPerformJaasLogin;
            return this;
        }

        /**
         * Sets the hostname to use in the Kerberos service principal.
         *
         * @param value the Kerberos service principal hostname
         * @return this request
         */
        public AuthenticationRequest kerberosServerHostname(String value) {
            this.kerberosServerHostname = value;
            return this;
        }
    }

    /**
     * Carries optional inputs formerly passed through the legacy argument list.
     *
     * @since 2026-08-01
     */
    public static class LegacyAuthenticationOptions {
        private final String jaasApplicationName;
        private final String kerberosServerName;
        private final boolean shouldUseSpnego;
        private final boolean shouldPerformJaasLogin;

        /**
         * Creates legacy authentication options.
         *
         * @param jaasApplicationName the JAAS application name
         * @param kerberosServerName the Kerberos service name
         * @param shouldUseSpnego true when SPNEGO should be preferred
         * @param shouldPerformJaasLogin true when JAAS login should be performed
         */
        public LegacyAuthenticationOptions(String jaasApplicationName, String kerberosServerName,
                boolean shouldUseSpnego, boolean shouldPerformJaasLogin) {
            this.jaasApplicationName = jaasApplicationName;
            this.kerberosServerName = kerberosServerName;
            this.shouldUseSpnego = shouldUseSpnego;
            this.shouldPerformJaasLogin = shouldPerformJaasLogin;
        }
    }

    /**
     * Authenticates with the legacy argument group.
     *
     * @param pgStream the PostgreSQL stream used for token exchange
     * @param host the current connection host
     * @param user the user being authenticated
     * @param password the secret used by the JAAS callback handler
     * @param options optional legacy authentication values
     * @throws IOException if socket communication fails during authentication
     * @throws SQLException if GSS authentication fails
     */
    public static void authenticate(PGStream pgStream, String host, String user, String password,
            LegacyAuthenticationOptions options) throws IOException, SQLException {
        LegacyAuthenticationOptions authenticationOptions =
                options == null ? new LegacyAuthenticationOptions(null, null, false, false)
                        : options;
        AuthenticationRequest request = new AuthenticationRequest(pgStream, host, user, password)
                .jaasApplicationName(authenticationOptions.jaasApplicationName)
                .kerberosServerName(authenticationOptions.kerberosServerName)
                .shouldUseSpnego(authenticationOptions.shouldUseSpnego)
                .shouldPerformJaasLogin(authenticationOptions.shouldPerformJaasLogin)
                .kerberosServerHostname(
                        System.getProperty(GssAction.KERBEROS_SERVER_HOSTNAME));
        authenticate(request);
    }

    /**
     * Authenticates with the supplied request data.
     *
     * @param request the authentication request
     * @throws IOException if socket communication fails during authentication
     * @throws SQLException if GSS authentication fails
     */
    public static void authenticate(AuthenticationRequest request)
            throws IOException, SQLException {
        logger.trace(" <=BE AuthenticationReqGSS");

        String jaasApplicationName =
                request.jaasApplicationName == null ? "pgjdbc" : request.jaasApplicationName;
        String kerberosServerName =
                request.kerberosServerName == null ? "postgres" : request.kerberosServerName;

        Exception result;
        try {
            boolean shouldPerformAuthentication = request.shouldPerformJaasLogin;
            GSSCredential gssCredential = null;
            Subject sub = Subject.getSubject(AccessController.getContext());
            if (sub != null) {
                Set<GSSCredential> gssCreds = sub.getPrivateCredentials(GSSCredential.class);
                if (gssCreds != null && !gssCreds.isEmpty()) {
                    gssCredential = gssCreds.iterator().next();
                    shouldPerformAuthentication = false;
                }
            }
            if (shouldPerformAuthentication) {
                LoginContext lc =
                        new LoginContext(jaasApplicationName,
                                new GSSCallbackHandler(request.user, request.credential));
                lc.login();
                sub = lc.getSubject();
            }
            PrivilegedAction<GssAction.AuthenticationResult> action =
                    new GssAction(request.pgStream, gssCredential, request.host, request.user,
                            kerberosServerName, request.shouldUseSpnego,
                            request.kerberosServerHostname);

            result = Subject.doAs(sub, action).getException();
        } catch (LoginException | SecurityException | IllegalArgumentException
                | NullPointerException e) {
            throw new PSQLException(GT.tr("GSS Authentication failed"),
                    PSQLState.CONNECTION_FAILURE, e);
        }

        if (result instanceof IOException) {
            throw (IOException) result;
        } else if (result instanceof SQLException) {
            throw (SQLException) result;
        } else if (result != null) {
            throw new PSQLException(GT.tr("GSS Authentication failed"),
                    PSQLState.CONNECTION_FAILURE, result);
        } else {
            logger.trace("GSS authentication completed without local exception");
        }
    }
}
