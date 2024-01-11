package net.starschema.clouddb.jdbc;

import java.util.Map;

public interface LabelledStatement {
  void setLabels(Map<String, String> labels);

  Map<String, String> getAllLabels();
}
