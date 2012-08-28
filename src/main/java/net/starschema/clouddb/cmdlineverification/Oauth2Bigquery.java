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
import com.google.api.client.auth.oauth2.MemoryCredentialStore;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.Bigquery.Builder;

public class Oauth2Bigquery{

	
		/**
	   * Browser to open in case {@link Desktop#isDesktopSupported()} is {@code false} or {@code null}
	   * to prompt user to open the URL in their favorite browser.
	   */
	  private static final String BROWSER = "google-chrome";

	  /** Google client secrets or {@code null} before initialized in {@link #authorize}. */
	  private static GoogleClientSecrets clientSecrets = null;

	  /** Returns the Google client secrets or {@code null} before initialized in {@link #authorize}. */
	  public static GoogleClientSecrets getClientSecrets() {
		  return clientSecrets;
	  }
	
	  public static GoogleAuthorizationCodeFlow codeflow = null;
	  
	  /**
	   * Creates GoogleClientsecrets "installed application" instance based on given Clientid, and Clientsecret
	   * @param jsonFactory
	   * @param clientid
	   * @param clientsecret
	   * @return GoogleClientsecrets of "installed application"
	   * @throws IOException
	   */
	private static GoogleClientSecrets loadClientSecrets(JsonFactory jsonFactory, String clientid, String clientsecret) throws IOException {
	    if (clientSecrets == null) {
	    String clientsecrets =	
	    	"{\n"+
	    		  "\"installed\": {\n"+
	    		    "\"client_id\": \""+clientid+"\",\n"+
	    		    "\"client_secret\":\""+clientsecret+"\",\n"+
	    		    "\"redirect_uris\": [\"http://localhost\", \"urn:ietf:oauth:2.0:oob\"],\n"+
	    		    "\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"+
	    		    "\"token_uri\": \"https://accounts.google.com/o/oauth2/token\"\n"+
	    		  "}\n"+
    		"}";
	      InputStream inputStream = new ByteArrayInputStream(clientsecrets.getBytes()); 
	      clientSecrets = GoogleClientSecrets.load(jsonFactory, inputStream);
	    }
	    return clientSecrets;
	  }
	
	 /**
	   * Authorizes the installed application to access user's protected data.
	   *
	   * @param transport HTTP transport
	   * @param jsonFactory JSON factory
	   * @param receiver verification code receiver
	   * @param scopes OAuth 2.0 scopes
	   */
	  public static Credential authorize(HttpTransport transport, JsonFactory jsonFactory,
	      VerificationCodeReceiver receiver, Iterable<String> scopes, String Clientid, String Clientsecret) throws Exception {
	    try {
	      String redirectUri = receiver.getRedirectUri();
	      GoogleClientSecrets clientSecrets = loadClientSecrets(jsonFactory,Clientid, Clientsecret);
	      codeflow = new GoogleAuthorizationCodeFlow.Builder(
	          transport, jsonFactory, clientSecrets, scopes).setAccessType("offline")
	          .setApprovalPrompt("auto").setCredentialStore(new MemoryCredentialStore()).build();
	      browse(codeflow.newAuthorizationUrl().setRedirectUri(redirectUri).build());
	      // receive authorization code and exchange it for an access token
	      String code = receiver.waitForCode();
	      GoogleTokenResponse response =
	    		  codeflow.newTokenRequest(code).setRedirectUri(redirectUri).execute();
	      // store credential and return it
	      
	      //Also ads a RefreshListener, so the token will be always automatically refreshed.
	      return codeflow.createAndStoreCredential(response, Clientid);
	    } finally {
	      receiver.stop();
	    }
	  }
	/**
	 * Authorizes a bigquery Connection with the given "Installed Application" Clientid and Clientsecret
	 * @param Clientid
	 * @param Clientsecret
	 * @return Authorized bigquery Connection
	 * @throws SQLException
	 */
	  public static Bigquery authorizeviainstalled(String Clientid, String Clientsecret) throws SQLException
	  {
		  LocalServerReceiver rcvr = new LocalServerReceiver();
		  List<String> Scopes = new ArrayList<String>();
			Scopes.add(BigqueryScopes.BIGQUERY);
			Credential credential = null;
			try {
				credential = Oauth2Bigquery.authorize(CmdlineUtils.getHttpTransport(), CmdlineUtils.getJsonFactory(), rcvr, Scopes, Clientid, Clientsecret);
			} catch (Exception e) {
				throw new SQLException(e);
			} 
		  Bigquery bigquery = new Builder(CmdlineUtils.getHttpTransport(), CmdlineUtils.getJsonFactory(), credential).build();
		return bigquery;
	  }
	  
	  /**
		 * This function gives back an Authorized Bigquery Client
		 * It uses a service account, which doesn't need user interaction for connect
		 * 
		 * @param serviceaccountemail
		 * @param keypath
		 * @return Authorized Bigquery Client via serviceaccount e-mail and keypath
		 * @throws GeneralSecurityException
		 * @throws IOException
		 */
		public static Bigquery authorizeviaservice(String serviceaccountemail, String keypath) throws GeneralSecurityException, IOException
		 {
		    GoogleCredential credential = new GoogleCredential.Builder().setTransport(CmdlineUtils.getHttpTransport())
		        .setJsonFactory(CmdlineUtils.getJsonFactory())
		        .setServiceAccountId(serviceaccountemail) //e-mail ADDRESS!!!!
		        .setServiceAccountScopes(BigqueryScopes.BIGQUERY) 
		        //Currently we only want to access bigquery, but it's possible to name more than one service too
		        .setServiceAccountPrivateKeyFromP12File(new File(keypath)) //keyfile, It's the users job keep it SAFE!
		        .build();
		   
		   Bigquery bigquery = new Builder(CmdlineUtils.getHttpTransport(), CmdlineUtils.getJsonFactory(), credential).build();
		   return bigquery;
		 }
	  
	  /** 
	   * Open a browser at the given URL. 
	   * @param url 
	   */
	  private static void browse(String url) {
	    // first try the Java Desktop
	    if (Desktop.isDesktopSupported()) {
	      Desktop desktop = Desktop.getDesktop();
	      if (desktop.isSupported(Action.BROWSE)) {
	        try {
	          desktop.browse(URI.create(url));
	          return;
	        } catch (IOException e) {
	          // handled below
	        }
	      }
	    }
	    // Next try rundll32 (only works on Windows)
	    try {
	      Runtime.getRuntime().exec("rundll32 url.dll,FileProtocolHandler " + url);
	      return;
	    } catch (IOException e) {
	      // handled below
	    }
	    // Next try the requested browser (e.g. "google-chrome")
	    if (BROWSER != null) {
	      try {
	        Runtime.getRuntime().exec(new String[] {BROWSER, url});
	        return;
	      } catch (IOException e) {
	        // handled below
	      }
	    }
	    // Finally just ask user to open in their browser using copy-paste
	    System.out.println("Please open the following URL in your browser:");
	    System.out.println("  " + url);
	  }
}
