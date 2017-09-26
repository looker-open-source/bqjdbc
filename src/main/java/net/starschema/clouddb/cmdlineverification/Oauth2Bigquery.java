/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This class implements functions to Authorize bigquery client
 */

package net.starschema.clouddb.cmdlineverification;

//import com.google.auth.oauth2.GoogleCredentials;
//import com.google.auth.oauth2.ServiceAccountCredentials;
//import com.google.cloud.bigquery.BigQuery;
//import com.google.cloud.bigquery.BigQueryOptions;
//import com.google.cloud.bigquery.Dataset;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Builder;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryRequestInitializer;
import com.google.api.services.bigquery.BigqueryScopes;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.awt.*;
import java.awt.Desktop.Action;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

public class Oauth2Bigquery {

    private static String servicepath = null;

    /**
     * Log4j logger, for debugging.
     */
    // static Logger logger = new Logger(Oauth2Bigquery.class.getName());
    static Logger logger = Logger.getLogger(Oauth2Bigquery.class.getName());
    /**
     * Browsers to try:
     */
    static final String[] browsers = {"google-chrome", "firefox", "opera",
            "epiphany", "konqueror", "conkeror", "midori", "kazehakase",
            "mozilla"};
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
    private static String PathForXmlStore = System.getProperty("user.home")
            + File.separator + ".bqjdbc" + File.separator
            + "xmllocation.properties";

    private static final String DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";

    /**
     * Authorizes the installed application to access user's protected data. if
     * possible, gets the credential from xml file at PathForXmlStore
     *
     * @param transport   HTTP transport
     * @param jsonFactory JSON factory
     * @param receiver    verification code receiver
     * @param scopes      OAuth 2.0 scopes
     */
    public static Credential authorize(HttpTransport transport,
                                       JsonFactory jsonFactory, VerificationCodeReceiver receiver,
                                       List<String> scopes, String clientid, String clientsecret)
            throws Exception {

        BQXMLCredentialStore Store = new BQXMLCredentialStore(
                Oauth2Bigquery.PathForXmlStore);

        GoogleClientSecrets.Details details = new Details();
        details.setClientId(clientid);
        details.setClientSecret(clientsecret);
        details.set("Factory", CmdlineUtils.getJsonFactory());
        details.setAuthUri("https://accounts.google.com/o/oauth2/auth");
        details.setTokenUri("https://accounts.google.com/o/oauth2/token");
        GoogleClientSecrets secr = new GoogleClientSecrets()
                .setInstalled(details);
        GoogleCredential CredentialForReturn = new GoogleCredential.Builder()
                .setJsonFactory(CmdlineUtils.getJsonFactory())
                .setTransport(CmdlineUtils.getHttpTransport())
                .setClientSecrets(secr).build();
        if (Store.load(clientid + ":" + clientsecret, CredentialForReturn) == true) {
            return CredentialForReturn;
        }
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
                                                 String clientsecret,
                                                 String userAgent) throws SQLException {
        LocalServerReceiver rcvr = new LocalServerReceiver();
        List<String> Scopes = new ArrayList<String>();
        Scopes.add(BigqueryScopes.BIGQUERY);
        Credential credential = null;
        try {
            logger.debug("Authorizing as installed app.");
            credential = Oauth2Bigquery.authorize(
                    CmdlineUtils.getHttpTransport(),
                    CmdlineUtils.getJsonFactory(), rcvr, Scopes, clientid,
                    clientsecret);
        } catch (Exception e) {
            throw new SQLException(e);
        }

        logger.debug("Creating a new bigquery client.");
        Builder bqBuilder = new Builder(
                CmdlineUtils.getHttpTransport(),
                CmdlineUtils.getJsonFactory(), credential
        );

        if (userAgent != null) {
            BigQueryRequestUserAgentInitializer requestInitializer = new BigQueryRequestUserAgentInitializer();
            requestInitializer.setUserAgent(userAgent);
            bqBuilder.setBigqueryRequestInitializer(requestInitializer);
        }

        Bigquery bigquery = bqBuilder.build();

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
                                               String keypath,
                                               String password,
                                               String userAgent,
                                               Integer connectTimeout,
                                               Integer readTimeout) throws GeneralSecurityException, IOException {
        logger.debug("Authorizing with service account.");

        /////////////////// NEW METHOD //////////

        // Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
        // environment variable, you can explicitly load the credentials file to construct the
        // credentials.


        //List<String> scopes = new ArrayList<String>();
        //scopes.add(BigqueryScopes.BIGQUERY);
        //// don't have access to DriveScopes without requiring the entire google drive sdk.
        //scopes.add(DRIVE_SCOPE);

        //GoogleCredentials credentials;
        //// JSON keypath
        //File credentialsPath = new File(keypath);
        //try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
        //    credentials = ServiceAccountCredentials.fromStream(serviceAccountStream).createScoped(scopes);
        //}

        //// Instantiate a client.
        //BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

        //Oauth2Bigquery.servicepath = bigquery.getServicePath();

        //// Use the client.
        //System.out.println("Datasets:");
        //for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
        //    System.out.printf("%s%n", dataset.getDatasetId().getDataset());
        //}

        ///////////////////////////////////////////
        List<String> scopes = new ArrayList<String>();
        scopes.add(BigqueryScopes.BIGQUERY);
        // don't have access to DriveScopes without requiring the entire google drive sdk.
        scopes.add(DRIVE_SCOPE);
        GoogleCredential.Builder builder = new GoogleCredential.Builder()
                .setTransport(CmdlineUtils.getHttpTransport())
                .setJsonFactory(CmdlineUtils.getJsonFactory())
                .setServiceAccountId(serviceaccountemail)
                        // e-mail ADDRESS!!!!
                .setServiceAccountScopes(scopes);
                        // Currently we only want to access bigquery, but it's possible
                        // to name more than one service too

        GoogleCredential credential = null;
        // If .p12 we load the PK into the builder before build
        if (Pattern.matches(".*\\.p12$", keypath)) {
            if (password == null) {
                builder = builder.setServiceAccountPrivateKeyFromP12File(new File(keypath));
            }
            else {
                PrivateKey pk = getPrivateKeyFromCredentials(keypath, password);
                builder = builder.setServiceAccountPrivateKey(pk);
            }
            credential = builder.build();
        }


        // If json then we load the PK with input stream after build
        if (Pattern.matches(".*\\.json$", keypath)) {
            File jsonKey = new File(keypath);
            InputStream inputStream = new FileInputStream(jsonKey);
            credential = credential.fromStream(inputStream, CmdlineUtils.getHttpTransport(), CmdlineUtils.getJsonFactory()).createScoped(scopes);
        }

        logger.debug("Authorizied?");

        HttpRequestTimeoutInitializer httpRequestInitializer = new HttpRequestTimeoutInitializer(credential);
        if (connectTimeout != null) {
            httpRequestInitializer.setConnectTimeout(connectTimeout);
        }
        if (readTimeout != null) {
            httpRequestInitializer.setReadTimeout(readTimeout);
        }

        Bigquery.Builder bqBuilder = new Builder(
                CmdlineUtils.getHttpTransport(),
                CmdlineUtils.getJsonFactory(),
                httpRequestInitializer)
                .setApplicationName("Starschema BigQuery JDBC Driver");

        if (userAgent != null) {
            BigQueryRequestUserAgentInitializer requestInitializer = new BigQueryRequestUserAgentInitializer();
            requestInitializer.setUserAgent(userAgent);

            bqBuilder.setBigqueryRequestInitializer(requestInitializer);
        }

        Bigquery bigquery = bqBuilder.build();

        Oauth2Bigquery.servicepath = bigquery.getServicePath();

        return bigquery;
    }

    private static PrivateKey getPrivateKeyFromCredentials(String keyPath, String password) throws GeneralSecurityException, IOException {
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        byte[] bytes = FileUtils.readFileToByteArray(new File(keyPath));

        keystore.load(new ByteArrayInputStream(bytes), password.toCharArray());
        return (PrivateKey)keystore.getKey(keystore.aliases().nextElement(), password.toCharArray());
    }

    /**
     * Open a browser at the given URL.
     *
     * @param url
     */
    private static void browse(String url) {
        // first try the Java Desktop
        logger.debug("First try the Java Desktop");
        if (Desktop.isDesktopSupported()) {
            Desktop desktop = Desktop.getDesktop();
            if (desktop.isSupported(Action.BROWSE)) {
                try {
                    desktop.browse(URI.create(url));
                    logger.debug("success");
                    return;
                } catch (IOException e) {
                    logger.debug("Failed with desktop.browse", e);
                    // handled below
                }
            }
        }


        // Next try browsers
        logger.debug("Try with browsers");
        // Code from Bare Bones Browser Launcher
        String osName = System.getProperty("os.name");
        try {
            if (osName.startsWith("Mac OS")) {
                logger.debug("Mac OS com.apple.eio.FileManager should handle the URL");
                Class.forName("com.apple.eio.FileManager")
                        .getDeclaredMethod("openURL",
                                new Class[]{String.class})
                        .invoke(null, new Object[]{url});
                return;
            } else if (osName.startsWith("Windows")) {
                logger.debug("Let's run internet suxplorer! with the URL: " + url);
                Runtime.getRuntime().exec(
                        "cmd.exe /c start iexplore.exe \"" + url + "\"");
                return;
            } else { // assume Unix or Linux-
                logger.debug("Unix or Linux, we'll open a browser");
                String browser = null;
                for (String b : browsers) {
                    if (browser == null
                            && Runtime.getRuntime()
                            .exec(new String[]{"which", b})
                            .getInputStream().read() != -1) {
                        Runtime.getRuntime().exec(
                                new String[]{browser = b, url});
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed", e);
            // handled below
        }

        logger.debug("Then try with url.connect");
        try {
            URL myURL = new URL(url);
            URLConnection myURLConnection = myURL.openConnection();
            myURLConnection.connect();
            logger.debug("success");
            return;
        } catch (Exception e) {
            logger.debug(null, e);
            // handled below
        }

        // Finally just ask user to open in their browser using copy-paste
        System.out.println("Please open the following URL in your browser:");
        System.out.println("  " + url);
        System.err.println("Please open the following URL in your browser:");
        System.err.println("  " + url);
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
            StringReader stringReader = new StringReader(clientsecrets);
            Oauth2Bigquery.clientSecrets = GoogleClientSecrets.load(
                    jsonFactory, stringReader);
        }
        return Oauth2Bigquery.clientSecrets;
    }

    private static class HttpRequestTimeoutInitializer implements HttpRequestInitializer {
        private Integer readTimeout = null;
        private Integer connectTimeout = null;
        private GoogleCredential credential = null;

        public HttpRequestTimeoutInitializer(GoogleCredential credential) {
            this.credential = credential;
        }

        public void setCredential(GoogleCredential credential) {
            this.credential = credential;
        }

        public void setReadTimeout(Integer timeout) {
            readTimeout = timeout;
        }

        public void setConnectTimeout(Integer timeout) {
            connectTimeout = timeout;
        }

        @Override
        public void initialize(HttpRequest httpRequest) throws IOException {
            credential.initialize(httpRequest);

            if (connectTimeout != null) {
                httpRequest.setConnectTimeout(connectTimeout);
            }
            if (readTimeout != null) {
                httpRequest.setReadTimeout(readTimeout);
            }
        }
    }

    private static class BigQueryRequestUserAgentInitializer extends BigqueryRequestInitializer {

        String userAgent = null;

        public void setUserAgent(String userAgent) {
            this.userAgent = userAgent;
        }

        public String getUserAgent() {
            return this.userAgent;
        }

        @Override
        public void initializeBigqueryRequest(BigqueryRequest<?> request) throws IOException {
            if (userAgent != null) {
                HttpHeaders currentHeaders = request.getRequestHeaders();

                currentHeaders.setUserAgent(userAgent);

                request.setRequestHeaders(currentHeaders);
            }
        }
    }
}
