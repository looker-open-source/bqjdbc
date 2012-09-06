/**
 *  Starschema Big Query JDBC Driver
 *  Copyright (C) 2012, Starschema Ltd.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 * This class implements functions to Authorize bigquery client
 */

package net.starschema.clouddb.cmdlineverification;

import java.awt.Desktop;
import java.awt.Desktop.Action;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Builder;
import com.google.api.services.bigquery.BigqueryScopes;

public class Oauth2Bigquery {

    private static String servicepath = null;

    /**
     * Browser to open in case {@link Desktop#isDesktopSupported()} is
     * {@code false} or {@code null} to prompt user to open the URL in their
     * favorite browser.
     */
    private static final String BROWSER = "google-chrome";

    /**
     * Google client secrets or {@code null} before initialized in
     * {@link #authorize}.
     */
    private static GoogleClientSecrets clientSecrets = null;

    /**
     * Reference to the GoogleAuthorizationCodeFlow used in this installed
     * application authorization sequence
     */
    public static GoogleAuthorizationCodeFlow codeflow = null;

    /**
     * The default path to the properties file that stores the xml file location
     * where client credentials are saved
     */
    private static String PathForXmlStore = "xmllocation.properties";

    /**
     * Authorizes the installed application to access user's protected data. if
     * possible, gets the credential from xml file at PathForXmlStore
     * 
     * @param transport
     *            HTTP transport
     * @param jsonFactory
     *            JSON factory
     * @param receiver
     *            verification code receiver
     * @param scopes
     *            OAuth 2.0 scopes
     */
    public static Credential authorize(HttpTransport transport,
            JsonFactory jsonFactory, VerificationCodeReceiver receiver,
            Iterable<String> scopes, String clientid, String clientsecret)
            throws Exception {

        BQXMLCredentialStore Store = new BQXMLCredentialStore(
                Oauth2Bigquery.PathForXmlStore);

        GoogleClientSecrets.Details details = new Details();
        details.setClientId(clientid);
        details.setClientSecret(clientsecret);
        details.setFactory(CmdlineUtils.getJsonFactory());
        details.setAuthUri("https://accounts.google.com/o/oauth2/auth");
        details.setTokenUri("https://accounts.google.com/o/oauth2/token");
        GoogleClientSecrets secr = new GoogleClientSecrets()
                .setInstalled(details);
        GoogleCredential CredentialForReturn = new GoogleCredential.Builder()
                .setJsonFactory(CmdlineUtils.getJsonFactory())
                .setTransport(CmdlineUtils.getHttpTransport())
                .setClientSecrets(secr).build();

        if (Store.load(clientid + ":" + clientsecret, CredentialForReturn) == true)
            return CredentialForReturn;
        try {
            String redirectUri = receiver.getRedirectUri();
            GoogleClientSecrets clientSecrets = Oauth2Bigquery
                    .loadClientSecrets(jsonFactory, clientid, clientsecret);
            Oauth2Bigquery.codeflow = new GoogleAuthorizationCodeFlow.Builder(
                    transport, jsonFactory, clientSecrets, scopes)
                    .setAccessType("offline").setApprovalPrompt("auto")
                    .setCredentialStore(Store).build();
            Oauth2Bigquery.browse(Oauth2Bigquery.codeflow.newAuthorizationUrl()
                    .setRedirectUri(redirectUri).build());
            // receive authorization code and exchange it for an access token
            String code = receiver.waitForCode();
            GoogleTokenResponse response = Oauth2Bigquery.codeflow
                    .newTokenRequest(code).setRedirectUri(redirectUri)
                    .execute();
            // store credential and return it

            // Also ads a RefreshListener, so the token will be always
            // automatically refreshed.
            return Oauth2Bigquery.codeflow.createAndStoreCredential(response,
                    clientid + ":" + clientsecret);
        } finally {
            receiver.stop();
        }
    }

    /**
     * Authorizes a bigquery Connection with the given "Installed Application"
     * Clientid and Clientsecret
     * 
     * @param clientid
     * @param clientsecret
     * @return Authorized bigquery Connection
     * @throws SQLException
     */
    public static Bigquery authorizeviainstalled(String clientid,
            String clientsecret) throws SQLException {
        LocalServerReceiver rcvr = new LocalServerReceiver();
        List<String> Scopes = new ArrayList<String>();
        Scopes.add(BigqueryScopes.BIGQUERY);
        Credential credential = null;
        try {
            credential = Oauth2Bigquery.authorize(
                    CmdlineUtils.getHttpTransport(),
                    CmdlineUtils.getJsonFactory(), rcvr, Scopes, clientid,
                    clientsecret);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        Bigquery bigquery = new Builder(CmdlineUtils.getHttpTransport(),
                CmdlineUtils.getJsonFactory(), credential).build();
        Oauth2Bigquery.servicepath = bigquery.getServicePath();
        return bigquery;
    }

    /**
     * This function gives back an Authorized Bigquery Client It uses a service
     * account, which doesn't need user interaction for connect
     * 
     * @param serviceaccountemail
     * @param keypath
     * @return Authorized Bigquery Client via serviceaccount e-mail and keypath
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static Bigquery authorizeviaservice(String serviceaccountemail,
            String keypath) throws GeneralSecurityException, IOException {
        GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(CmdlineUtils.getHttpTransport())
                .setJsonFactory(CmdlineUtils.getJsonFactory())
                .setServiceAccountId(serviceaccountemail)
                // e-mail ADDRESS!!!!
                .setServiceAccountScopes(BigqueryScopes.BIGQUERY)
                // Currently we only want to access bigquery, but it's possible
                // to name more than one service too
                .setServiceAccountPrivateKeyFromP12File(new File(keypath))
                .build();

        Bigquery bigquery = new Builder(CmdlineUtils.getHttpTransport(),
                CmdlineUtils.getJsonFactory(), credential).build();
        Oauth2Bigquery.servicepath = bigquery.getServicePath();
        return bigquery;
    }

    /**
     * Open a browser at the given URL.
     * 
     * @param url
     */
    private static void browse(String url) {
        // first try the Java Desktop
        if (Desktop.isDesktopSupported()) {
            Desktop desktop = Desktop.getDesktop();
            if (desktop.isSupported(Action.BROWSE))
                try {
                    desktop.browse(URI.create(url));
                    return;
                } catch (IOException e) {
                    // handled below
                }
        }
        // Next try rundll32 (only works on Windows)
        try {
            Runtime.getRuntime().exec(
                    "rundll32 url.dll,FileProtocolHandler " + url);
            return;
        } catch (IOException e) {
            // handled below
        }
        // Next try the requested browser (e.g. "google-chrome")
        if (Oauth2Bigquery.BROWSER != null)
            try {
                Runtime.getRuntime().exec(
                        new String[] { Oauth2Bigquery.BROWSER, url });
                return;
            } catch (IOException e) {
                // handled below
            }
        // Finally just ask user to open in their browser using copy-paste
        System.out.println("Please open the following URL in your browser:");
        System.out.println("  " + url);
    }

    /**
     * Returns the Google client secrets or {@code null} before initialized in
     * {@link #authorize}.
     */
    public static GoogleClientSecrets getClientSecrets() {
        return Oauth2Bigquery.clientSecrets;
    }

    public static String getservicepath() {
        return Oauth2Bigquery.servicepath;
    }

    /**
     * Creates GoogleClientsecrets "installed application" instance based on
     * given Clientid, and Clientsecret
     * 
     * @param jsonFactory
     * @param clientid
     * @param clientsecret
     * @return GoogleClientsecrets of "installed application"
     * @throws IOException
     */
    private static GoogleClientSecrets loadClientSecrets(
            JsonFactory jsonFactory, String clientid, String clientsecret)
            throws IOException {
        if (Oauth2Bigquery.clientSecrets == null) {
            String clientsecrets = "{\n"
                    + "\"installed\": {\n"
                    + "\"client_id\": \""
                    + clientid
                    + "\",\n"
                    + "\"client_secret\":\""
                    + clientsecret
                    + "\",\n"
                    + "\"redirect_uris\": [\"http://localhost\", \"urn:ietf:oauth:2.0:oob\"],\n"
                    + "\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
                    + "\"token_uri\": \"https://accounts.google.com/o/oauth2/token\"\n"
                    + "}\n" + "}";
            InputStream inputStream = new ByteArrayInputStream(
                    clientsecrets.getBytes());
            Oauth2Bigquery.clientSecrets = GoogleClientSecrets.load(
                    jsonFactory, inputStream);
        }
        return Oauth2Bigquery.clientSecrets;
    }
}
