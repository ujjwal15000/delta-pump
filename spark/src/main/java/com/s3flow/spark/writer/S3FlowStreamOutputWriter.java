package com.s3flow.spark.writer;

import static com.s3flow.spark.util.ByteArrayUtils.convertToByteArray;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructType;

public class S3FlowStreamOutputWriter extends OutputWriter {
  private final String path;
  private final StructType schema;
  private final TaskAttemptContext context;

  private final OutputStream outputStream;

  public S3FlowStreamOutputWriter(String path, StructType schema, TaskAttemptContext context) {
    this.path = path;
    this.schema = schema;
    this.context = context;

    Path p = new Path(path);
    this.outputStream = CodecStreams.createOutputStream(context, p);
  }

  @Override
  public void write(InternalRow row) {
    byte[] key = row.getBinary(0);
    byte[] value = row.getBinary(1);

    try {
      outputStream.write(convertToByteArray(4 + 4 + key.length + 4 + value.length));
      outputStream.write(convertToByteArray(key.length));
      outputStream.write(key);
      outputStream.write(convertToByteArray(value.length));
      outputStream.write(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      outputStream.flush();
      outputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String path() {
    return this.path;
  }
}
