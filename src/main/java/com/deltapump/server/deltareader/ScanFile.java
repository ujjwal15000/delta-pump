package com.deltapump.server.deltareader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.delta.kernel.data.Row;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
public class ScanFile {
    private String stateJson;
    private String fileJson;

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

    @JsonIgnore
    public Row getScanRow() {
      return RowSerDe.deserializeRowFromJson(stateJson);
    }

    @JsonIgnore
    public Row getScanFileRow() {
      return RowSerDe.deserializeRowFromJson(fileJson);
    }

    public String toString() {
        return stateJson + "__" + fileJson;
    }
  }