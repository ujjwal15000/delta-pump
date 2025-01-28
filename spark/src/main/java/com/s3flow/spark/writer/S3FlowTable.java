package com.s3flow.spark.writer;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class S3FlowTable implements SupportsWrite {
  private final StructType schema;
  private final String tableName;
  private final Map<String, String> properties;

  public S3FlowTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.tableName = properties.get("tableName");
    this.properties = properties;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of(TableCapability.BATCH_WRITE);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return null;
  }
}
