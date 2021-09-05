/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * <p>This class implements functions to Authorize bigquery client
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Builder;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryRequestInitializer;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.MinifiedBigquery;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Oauth2Bigquery {

  /** Global instance of the HTTP transport. */
  static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  /** Log4j logger, for debugging. */
  static Logger logger = LoggerFactory.getLogger(Oauth2Bigquery.class);
  /** Browsers to try: */
  static final String[] browsers = {
    "google-chrome",
    "firefox",
    "opera",
    "epiphany",
    "konqueror",
    "conkeror",
    "midori",
    "kazehakase",
    "mozilla"
  };
  /** Application name set on bigquery connection */
  static final String applicationName = "BigQuery JDBC Driver";

  private static final String DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";

  private static final Integer MAX_IMPERSONATION_LIFETIME = 43200;

  /**
   * Creates a Bigquery.Builder using the provided GoogleCredential
   *
   * @param credential a valid GoogleCredential
   * @return Bigquery.Builder suitable for initalizing a MinifiedBigquery
   */
  private static Bigquery.Builder createBqBuilderForCredential(
      GoogleCredentials credential,
      Integer connectTimeout,
      Integer readTimeout,
      HttpTransport httpTransport,
      String userAgent,
      String rootUrl,
      String targetServiceAccount) {

    if (targetServiceAccount != null) {
      credential = impersonateServiceAccount(credential, targetServiceAccount);
    }

    HttpRequestTimeoutInitializer httpRequestInitializer =
        createRequestTimeoutInitalizer(credential, connectTimeout, readTimeout);

    Bigquery.Builder bqBuilder =
        new Builder(httpTransport, JSON_FACTORY, httpRequestInitializer)
            .setApplicationName(applicationName);

    if (userAgent != null) {
      BigQueryRequestUserAgentInitializer requestInitializer =
          new BigQueryRequestUserAgentInitializer();
      requestInitializer.setUserAgent(userAgent);

      bqBuilder.setBigqueryRequestInitializer(requestInitializer);
    }

    if (rootUrl != null) {
      bqBuilder.setRootUrl(rootUrl);
    }

    return bqBuilder;
  }

  /**
   * Helper method to create a HttpRequestTimeoutInitializer
   *
   * @param credential a valid GoogleCredential
   * @return HttpRequestTimeoutInitializer suitable for use with Bigquery.Builder
   */
  private static HttpRequestTimeoutInitializer createRequestTimeoutInitalizer(
      GoogleCredentials credential, Integer connectTimeout, Integer readTimeout) {
    HttpRequestTimeoutInitializer httpRequestInitializer =
        new HttpRequestTimeoutInitializer(credential);
    if (connectTimeout != null) {
      httpRequestInitializer.setConnectTimeout(connectTimeout);
    }
    if (readTimeout != null) {
      httpRequestInitializer.setReadTimeout(readTimeout);
    }

    return httpRequestInitializer;
  }

  /**
   * Authorizes a bigquery Connection with the given OAuth 2.0 Access Token
   *
   * @param oauthToken
   * @return Authorized Bigquery Connection via OAuth Token
   * @throws SQLException
   */
  public static Bigquery authorizeViaToken(
      String oauthToken,
      String userAgent,
      Integer connectTimeout,
      Integer readTimeout,
      String rootUrl,
      HttpTransport httpTransport,
      String targetServiceAccount)
      throws SQLException {
    GoogleCredentials credential = GoogleCredentials.create(new AccessToken(oauthToken, null));

    logger.debug("Creating a new bigquery client.");

    Bigquery.Builder bqBuilder =
        createBqBuilderForCredential(
            credential,
            connectTimeout,
            readTimeout,
            httpTransport,
            userAgent,
            rootUrl,
            targetServiceAccount);

    return new MinifiedBigquery(bqBuilder);
  }

  /**
   * This function gives back an built GoogleCredentials Object from a p12 keyfile
   *
   * @param serviceaccountemail
   * @param keypath
   * @return Built GoogleCredentials via serviceaccount e-mail and keypath
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static GoogleCredentials createP12Credential(
      String serviceaccountemail, String keypath, String password, boolean forTokenGeneration)
      throws GeneralSecurityException, IOException {
    logger.debug("Authorizing with service account.");
    ServiceAccountCredentials.Builder builder =
        ServiceAccountCredentials.newBuilder()
            .setClientEmail(serviceaccountemail)
            // e-mail ADDRESS!!!!
            .setScopes(GenerateScopes(forTokenGeneration));
    // Currently we only want to access bigquery, but it's possible
    // to name more than one service too

    if (password == null) {
      password = "notasecret";
    }
    PrivateKey pk = getPrivateKeyFromCredentials(keypath, password);
    builder = builder.setPrivateKey(pk);
    return builder.build();
  }

  /**
   * This function gives back an built GoogleCredentials Object from a String representing the
   * contents of a JSON keyfile
   *
   * @param jsonAuthContents
   * @return Built GoogleCredentials via and keypath
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static GoogleCredentials createJsonCredential(
      String jsonAuthContents, boolean forTokenGeneration)
      throws GeneralSecurityException, IOException {
    logger.debug("Authorizing with service account.");
    // For .json load the key via credential.fromStream
    InputStream stringStream = new ByteArrayInputStream(jsonAuthContents.getBytes());
    return GoogleCredentials.fromStream(stringStream)
        .createScoped(GenerateScopes(forTokenGeneration));
  }

  /**
   * This function gives back an built GoogleCredentials Object from a json keyfile
   *
   * @param keypath
   * @return Built GoogleCredentials via and keypath
   * @throws GeneralSecurityException
   * @throws IOException
   */
  private static GoogleCredentials createJsonCredentialFromKeyfile(
      String keypath, boolean forTokenGeneration) throws GeneralSecurityException, IOException {
    logger.debug("Authorizing with service account.");
    // For .json load the key via credential.fromStream
    File jsonKey = new File(keypath);
    InputStream inputStream = new FileInputStream(jsonKey);
    return GoogleCredentials.fromStream(inputStream)
        .createScoped(GenerateScopes(forTokenGeneration));
  }

  /**
   * This function gives back an Authorized Bigquery Client It uses a service account, which doesn't
   * need user interaction for connect
   *
   * @param serviceaccountemail
   * @param keypath
   * @param jsonAuthContents
   * @return Authorized Bigquery Client via serviceaccount e-mail and keypath
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static Bigquery authorizeViaService(
      String serviceaccountemail,
      String keypath,
      String password,
      String userAgent,
      String jsonAuthContents,
      Integer readTimeout,
      Integer connectTimeout,
      String rootUrl,
      HttpTransport httpTransport,
      String targetServiceAccount)
      throws GeneralSecurityException, IOException {
    GoogleCredentials credential =
        createServiceAccountCredential(
            serviceaccountemail, keypath, password, jsonAuthContents, false);

    logger.debug("Authorized?");

    Bigquery.Builder bqBuilder =
        createBqBuilderForCredential(
            credential,
            connectTimeout,
            readTimeout,
            httpTransport,
            userAgent,
            rootUrl,
            targetServiceAccount);

    return new MinifiedBigquery(bqBuilder);
  }

  /**
   * This function gives back an Authorized Bigquery Client using the Application Default
   * Credentials. https://cloud.google.com/docs/authentication/production#automatically
   *
   * <p>The following are searched (in order) to find the Application Default Credentials:
   *
   * <ol>
   *   <li>Credentials file pointed to by the {@code GOOGLE_APPLICATION_CREDENTIALS} environment
   *       variable
   *   <li>Credentials provided by the Google Cloud SDK.
   *       <ol>
   *         <li>{@code gcloud auth application-default login} for user account credentials.
   *         <li>{@code gcloud auth application-default login --impersonate-service-account} for
   *             impersonated service account credentials.
   *       </ol>
   *   <li>Google App Engine built-in credentials
   *   <li>Google Cloud Shell built-in credentials
   *   <li>Google Compute Engine built-in credentials
   * </ol>
   *
   * @return Authorized Bigquery Client via serviceaccount e-mail and keypath
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static Bigquery authorizeViaApplicationDefault(
      String userAgent,
      Integer connectTimeout,
      Integer readTimeout,
      String rootUrl,
      HttpTransport httpTransport,
      String targetServiceAccount)
      throws IOException {
    GoogleCredentials credential = GoogleCredentials.getApplicationDefault();

    logger.debug("Authorizing with Application Default Credentials.");

    Bigquery.Builder bqBuilder =
        createBqBuilderForCredential(
            credential,
            connectTimeout,
            readTimeout,
            httpTransport,
            userAgent,
            rootUrl,
            targetServiceAccount);

    return new MinifiedBigquery(bqBuilder);
  }

  /**
   * This function gives back a valid OAuth 2.0 access token from service account credentials
   *
   * @param serviceaccountemail
   * @param keypath
   * @param password
   * @param jsonAuthContents
   * @return Valid OAuth 2.0 access token
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static String generateAccessToken(
      String serviceaccountemail, String keypath, String password, String jsonAuthContents)
      throws GeneralSecurityException, IOException {
    GoogleCredentials credential =
        createServiceAccountCredential(
            serviceaccountemail, keypath, password, jsonAuthContents, true);
    HttpRequestTimeoutInitializer httpRequestInitializer =
        new HttpRequestTimeoutInitializer(credential);

    IAMCredentials.Builder builder =
        new IAMCredentials.Builder(HTTP_TRANSPORT, JSON_FACTORY, httpRequestInitializer)
            .setApplicationName(applicationName);

    IAMCredentials iamCredentials = builder.build();

    String name = "projects/-/serviceAccounts/" + serviceaccountemail;
    GenerateAccessTokenRequest request = new GenerateAccessTokenRequest();
    request.setScope(Collections.singletonList(BigqueryScopes.CLOUD_PLATFORM));

    IAMCredentials.Projects.ServiceAccounts.GenerateAccessToken generateAccessToken;
    generateAccessToken =
        iamCredentials.projects().serviceAccounts().generateAccessToken(name, request);
    GenerateAccessTokenResponse response = generateAccessToken.execute();
    return response.getAccessToken();
  }

  private static GoogleCredentials impersonateServiceAccount(
      GoogleCredentials sourceCredentials, String targetServiceAccount) {

    return ImpersonatedCredentials.create(
        sourceCredentials,
        targetServiceAccount,
        null, // Look into delegates later if necessary
        GenerateScopes(false),
        MAX_IMPERSONATION_LIFETIME);
  }

  private static GoogleCredentials createServiceAccountCredential(
      String serviceaccountemail,
      String keypath,
      String password,
      String jsonAuthContents,
      boolean forTokenGeneration)
      throws GeneralSecurityException, IOException {
    GoogleCredentials credential;
    // Determine which keyfile we are trying to authenticate with.
    if (jsonAuthContents != null) {
      credential = Oauth2Bigquery.createJsonCredential(jsonAuthContents, forTokenGeneration);
    } else if (Pattern.matches(".*\\.json$", keypath)) {
      // For backwards compat: this is no longer the preferred path for JSON (better to use
      // [jsonAuthContents]
      credential = Oauth2Bigquery.createJsonCredentialFromKeyfile(keypath, forTokenGeneration);
    } else {
      credential =
          Oauth2Bigquery.createP12Credential(
              serviceaccountemail, keypath, password, forTokenGeneration);
    }
    return credential;
  }

  // Helper function to generate scopes for credential files
  private static List<String> GenerateScopes(boolean forTokenGeneration) {
    List<String> scopes = new ArrayList<String>();
    if (forTokenGeneration) {
      scopes.add(BigqueryScopes.CLOUD_PLATFORM);
    } else {
      scopes.add(BigqueryScopes.BIGQUERY);
      // don't have access to DriveScopes without requiring the entire google drive sdk.
      scopes.add(DRIVE_SCOPE);
    }
    return scopes;
  }

  private static PrivateKey getPrivateKeyFromCredentials(String keyPath, String password)
      throws GeneralSecurityException, IOException {
    KeyStore keystore = KeyStore.getInstance("PKCS12");
    byte[] bytes = FileUtils.readFileToByteArray(new File(keyPath));

    keystore.load(new ByteArrayInputStream(bytes), password.toCharArray());
    return (PrivateKey) keystore.getKey(keystore.aliases().nextElement(), password.toCharArray());
  }

  private static class HttpRequestTimeoutInitializer extends HttpCredentialsAdapter {
    private Integer readTimeout = null;
    private Integer connectTimeout = null;

    public HttpRequestTimeoutInitializer(GoogleCredentials credential) {
      super(credential);
    }

    public void setReadTimeout(Integer timeout) {
      readTimeout = timeout;
    }

    public void setConnectTimeout(Integer timeout) {
      connectTimeout = timeout;
    }

    @Override
    public void initialize(HttpRequest httpRequest) throws IOException {
      super.initialize(httpRequest);

      if (connectTimeout != null) {
        httpRequest.setConnectTimeout(connectTimeout);
      }
      if (readTimeout != null) {
        httpRequest.setReadTimeout(readTimeout);
      }
    }

    @Override
    public boolean handleResponse(
        HttpRequest request, HttpResponse response, boolean supportsRetry) {
      try {
        return super.handleResponse(request, response, supportsRetry);
      } catch (IllegalStateException ise) {
        // There's an ugly interaction between HttpCredentialsAdapter and OAuth2Credentials (from
        // GoogleCredentials).  On a failed request, it will attempt to refresh the access token,
        // but OAuth2Credentials class doesn't support refresh and will throw an
        // IllegalStateException instead of returning an Unauthorized response.  Capture that
        // exception and return false so we don't retry.
        return false;
      }
    }
  }

  private static class BigQueryRequestUserAgentInitializer extends BigqueryRequestInitializer {

    String userAgent = null;
    String oauthToken = null;

    public void setUserAgent(String userAgent) {
      this.userAgent = userAgent;
    }

    public void setOauthToken(String oauthToken) {
      this.oauthToken = oauthToken;
    }

    public String getOauthToken() {
      return this.oauthToken;
    }

    @Override
    public void initializeBigqueryRequest(BigqueryRequest<?> request) throws IOException {
      if (userAgent != null) {
        HttpHeaders currentHeaders = request.getRequestHeaders();

        currentHeaders.setUserAgent(userAgent);

        request.setRequestHeaders(currentHeaders);
      }
      if (oauthToken != null) {
        request.setOauthToken(oauthToken);
      }
    }
  }
}
