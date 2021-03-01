package com.google.api.services.bigquery;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.json.MockJsonFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class MinifiedBigqueryTest {

    @Test
    public void testMinifiedDisabledByDefault() throws Exception {
        MockHttpTransport httpTransport = new MockHttpTransport();
        MockJsonFactory jsonFactory = new MockJsonFactory();
        MinifiedBigquery bigquery = new MinifiedBigquery(new Bigquery.Builder(httpTransport, jsonFactory,request -> {

        }));

        HttpRequest request = bigquery.datasets().list("123").buildHttpRequest();
        Assertions.assertThat(request.getUrl().getFirst("prettyPrint")).isEqualTo("false");
    }

    @Test
    public void testMinifiedCanBeEnabled() throws Exception {
        MockHttpTransport httpTransport = new MockHttpTransport();
        MockJsonFactory jsonFactory = new MockJsonFactory();
        MinifiedBigquery bigquery = new MinifiedBigquery(new Bigquery.Builder(httpTransport, jsonFactory,request -> {

        }), true);

        HttpRequest request = bigquery.datasets().list("123").buildHttpRequest();
        Assertions.assertThat(request.getUrl().getFirst("prettyPrint")).isEqualTo("true");
    }
}
