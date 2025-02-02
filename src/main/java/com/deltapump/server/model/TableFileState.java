package com.deltapump.server.model;

import com.deltapump.server.deltareader.ScanFile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableFileState {
    private Integer fileId;
    private ScanFile scanFile;
    private State state;

    public enum State{
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED
    }
}
