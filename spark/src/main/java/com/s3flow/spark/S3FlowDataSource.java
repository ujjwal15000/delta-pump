package com.s3flow.spark;

import com.s3flow.spark.writer.S3FlowFileFormat;
import com.s3flow.spark.writer.S3FlowTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class S3FlowDataSource implements DataSourceRegister, FileDataSourceV2 {
    public static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
    });

    @Override
    public String shortName() {
        return "s3-flow";
    }

    @Override
    public Class<? extends FileFormat> fallbackFileFormat() {
        return S3FlowFileFormat.class;
    }

    @Override
    public Table getTable(CaseInsensitiveStringMap options) {
        return new S3FlowTable(DEFAULT_SCHEMA, options.asCaseSensitiveMap());
    }

    @Override
    public Table org$apache$spark$sql$execution$datasources$v2$FileDataSourceV2$$t() {
        return null;
    }

    @Override
    public void org$apache$spark$sql$execution$datasources$v2$FileDataSourceV2$$t_$eq(Table x$1) {

    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return DEFAULT_SCHEMA;
    }

}

