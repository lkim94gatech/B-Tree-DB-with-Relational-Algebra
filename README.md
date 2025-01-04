# B-Tree-DB-with-Relational-Algebra

**BTreeDB** is a lightweight, persistent relational database system built using modern C++. It implements core database functionalities, including **buffer management**, **slotted pages**, and **relational algebra operations**. The system supports multi-threading and ensures data persistence while providing robust query execution and testing frameworks.

---

## Key Features

### 1. **Relational Algebra Operators**
- **Projection**: Select specific attributes from tuples.
- **Selection**: Filter tuples based on conditions.
- **Join**: Perform inner joins using hash-based algorithms.
- **Aggregation**: Compute `SUM`, `MIN`, `MAX`, and `COUNT`.
- **Set Operations**:
  - **Union** and **Union All**
  - **Intersect** and **Intersect All**
  - **Except** and **Except All**
- **Sorting**: Multi-criteria tuple sorting with ascending or descending order.

### 2. **Buffer Management**
- Implements a **Least Recently Used (LRU)** eviction policy.
- Efficient in-memory page management for database operations.
- Persistent storage via slotted pages.

### 3. **Persistence**
- Ensures database integrity across program executions.
- Uses slotted pages for dynamic tuple storage and retrieval.

### 4. **Query Parsing and Execution**
- Parse SQL-like queries to execute relational algebra operations.
- Supports `WHERE`, `GROUP BY`, `SUM`, and `JOIN` clauses.

### 5. **Testing Framework**
- Validates individual features with unit tests:
  - Field comparison
  - Projection
  - Sorting
  - Hash joins
  - Aggregation
  - Set operations (Union, Intersect, Except)
  - Query execution

---

## Getting Started

### Prerequisites
- **C++ Compiler**: Requires a modern C++ compiler supporting **C++17** or later.
- **Build Tools**: `g++` or equivalent.

### Compilation
1. Clone the repository or save the code as `btreedb.cpp`.
2. Compile the code using the following command:
   ```bash
   g++ -std=c++17 -pthread -o btreedb btreedb.cpp
