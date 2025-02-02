# DeltaPump

DeltaPump is a horizontally scalable and elastic service that continuously reads from a Delta Table without requiring Apache Spark. It leverages Apache Helix for partition management and Delta Kernel for efficient data access.

### Features

- Spark-Free: Reads directly from Delta Tables without the need for Apache Spark.

- Horizontally Scalable: Dynamically scales to accommodate increasing workloads.

- Elastic: Adapts to changing loads by rebalancing partitions across nodes.

- Partition Management: Uses Apache Helix for dynamic partition assignment and fault tolerance.

- Delta Kernel Integration: Efficiently processes Delta Tables without a Spark dependency.

### Architecture

DeltaPump consists of multiple nodes that coordinate using Apache Helix to ensure balanced partition distribution. Each node processes assigned partitions from the Delta Table using Delta Kernel.

### Components

- Delta Reader: Uses Delta Kernel to read data incrementally.

- Helix Controller: Manages partition distribution and rebalancing.

- Worker Nodes: Process assigned partitions and emit results downstream.

### Getting Started

Prerequisites

- Java 11+

- Apache Helix

- Delta Kernel

- A configured Delta Table as the data source

### Installation
#### Clone the repository
git clone https://github.com/ujjwal15000/delta-pump.git
cd delta-pump

#### Build the project
mvn clean package

#### Configuration
run the packaged jar with

-DzkHost="localhost:2181,localhost:2182"
-DdeltaTable.path="/path/to/delta-table"

#### Contributing

I welcome contributions! Please submit issues and pull requests on GitHub.

#### License

This project is licensed under the [MIT](https://opensource.org/license/MIT) License.

Copyright (c) 2025-present, Ujjwal Bagrania