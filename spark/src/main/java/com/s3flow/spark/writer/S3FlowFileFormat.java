package com.s3flow.spark.writer;

import static com.s3flow.spark.S3FlowDataSource.DEFAULT_SCHEMA;

import java.util.Objects;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class S3FlowFileFormat implements FileFormat {

  @Override
  public Option<StructType> inferSchema(
      SparkSession sparkSession, Map<String, String> options, Seq<FileStatus> files) {
    return null;
  }

  @Override
  public OutputWriterFactory prepareWrite(
      SparkSession sparkSession, Job job, Map<String, String> options, StructType dataSchema) {
    verifySchema(dataSchema);
    return new SyncDbStreamOutputWriterFactory();
  }

  @Override
  public boolean supportBatch(SparkSession sparkSession, StructType dataSchema) {
    return true;
  }

  @Override
  public boolean isSplitable(SparkSession sparkSession, Map<String, String> options, Path path) {
    return false;
  }

  @Override
  public boolean supportFieldName(String name) {
    return FileFormat.super.supportFieldName(name);
  }

  private static void verifySchema(StructType schema){
    if (!Objects.deepEquals(schema, DEFAULT_SCHEMA))
      throw new RuntimeException(
              "schema mismatch detected. Expected schema should be:\n\t"
                      + DEFAULT_SCHEMA
                      + "\n\treceived schema:\n\t"
                      + schema);
  }
}
