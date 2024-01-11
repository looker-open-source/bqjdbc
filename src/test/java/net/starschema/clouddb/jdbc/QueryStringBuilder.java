package net.starschema.clouddb.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class QueryStringBuilder {
  private final List<String> parameters = new ArrayList<>();
  private final Charset encoding;

  public QueryStringBuilder() {
    this(StandardCharsets.UTF_8);
  }

  public QueryStringBuilder(final Charset encoding) {
    this.encoding = encoding;
  }

  private String encode(final String segment) {
    try {
      return URLEncoder.encode(segment, encoding.displayName());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryStringBuilder put(final String key, final String value) {
    final String pair = encode(key) + "=" + encode(value);
    parameters.add(pair);
    return this;
  }

  public String build() {
    return String.join("&", parameters);
  }
}
