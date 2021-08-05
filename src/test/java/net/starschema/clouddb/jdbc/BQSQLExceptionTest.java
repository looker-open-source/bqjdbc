package net.starschema.clouddb.jdbc;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class BQSQLExceptionTest {

    @Test
    public void exceptionCauseEnrichmentIOExceptionTest() {
        IOException ioException = new IOException("Read timed out!");
        BQSQLException exception = new BQSQLException("Oops! Something went wrong:", ioException);

        String actualMessage = exception.getMessage();
        Assert.assertEquals("Oops! Something went wrong: - java.io.IOException: Read timed out!", actualMessage);
    }

    @Test
    public void exceptionCauseEnrichmentGoogleJsonResponseExceptionTest() {
        HttpHeaders headers = new HttpHeaders();
        HttpResponseException.Builder builder = new HttpResponseException.Builder(
            403,"you can't see this", headers
        );
        GoogleJsonError error = new GoogleJsonError();
        error.setMessage("You don't have access");
        GoogleJsonResponseException cause = new GoogleJsonResponseException(builder, error);
        BQSQLException exception = new BQSQLException("Oops! Something went wrong:", cause);

        String actualMessage = exception.getMessage();
        Assert.assertEquals("Oops! Something went wrong: - You don't have access", actualMessage);
    }

    @Test
    public void exceptionCauseEnrichmentOtherRuntimeExceptionTest() {
        Exception exception = new RuntimeException("something went horribly wrong");
        BQSQLException sqlException = new BQSQLException("Oops! Something went wrong:", exception);

        String actualMessage = sqlException.getMessage();
        Assert.assertEquals("Oops! Something went wrong: - java.lang.RuntimeException: something went horribly wrong", actualMessage);
    }

}
