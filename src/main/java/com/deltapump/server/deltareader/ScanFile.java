package com.deltapump.server.deltareader;

import io.delta.kernel.data.Row;
import lombok.Getter;

@Getter
public class ScanFile {
    final String stateJson;
    final String fileJson;

    public ScanFile(Row scanStateRow, Row scanFileRow) {
      this.stateJson = RowSerDe.serializeRowToJson(scanStateRow);
      this.fileJson = RowSerDe.serializeRowToJson(scanFileRow);
    }

    public ScanFile(String config) {
        this.stateJson = config.split("__")[0];
        this.fileJson = config.split("__")[1];
    }

    public ScanFile(String stateJson, String fileJson) {
      this.stateJson = stateJson;
      this.fileJson = fileJson;
    }

    public Row getScanRow() {
      return RowSerDe.deserializeRowFromJson(stateJson);
    }

    public Row getScanFileRow() {
      return RowSerDe.deserializeRowFromJson(fileJson);
    }

    public String toString() {
        return stateJson + "__" + fileJson;
    }
  }