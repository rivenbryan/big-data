# Columnar Storage System (Java)

A simplified in-memory **columnar storage engine** written in Java that supports:

- **Partitioning** on a specific column (e.g., `month`)
- **Zone Map Indexing** for fast range scans
- **Shared Scan** and **Normal Scan** execution modes
- Filtering by value, date range, or numeric range
- **Automatic preprocessing** based on a given matric number
- Writes filtered **results to a CSV file**

---

## Project Structure

```
proj/
├── Main.java               # Entry point, accepts CLI arguments and delegates to scan modes
├── ColumnStore.java        # Core column-store engine: handles data loading, filtering, indexing
├── RunSharedScan.java      # Executes scan using shared scan logic
├── RunNormal.java          # Executes scan without shared scan logic
├── Util.java               # Helper methods: preprocessing matric number, CSV output, casting
├── Statistics.java         # Computes stats like min, avg, std dev, price per sqm
├── Block.java              # Represents a fixed-size column block, with optional zone map
├── Partition.java          # Manages column blocks grouped by partition keys (e.g., town)
├── Disk.java               # In-memory disk abstraction mapping columns/partitions to blocks
├── Constant.java           # Stores constants: column names, file path, area thresholds
```

---

## How to Run

### 1. Compile all Java files

```bash
javac -d out $(find . -name "*.java")
```

### 2. Run the program with your **matric number** and optional flags

```bash
# Basic usage
java -cp out proj.Main A1234567X

# Disable shared scan
java -cp out proj.Main A1234567X sharedScan=false

# Use partitioning and zone map indexing
java -cp out proj.Main A1234567X partitionBy=month zoneMapBy=floor_area_sqm

# Full example
java -cp out proj.Main A1234567X sharedScan=true partitionBy=month zoneMapBy=floor_area_sqm
```

---

## Output

After filtering and processing, the program will:

- Print statistics in the console
- Write results to a CSV file:

```
ScanResult_A1234567X.csv
```

---

## Notes

- The `matricNumber` is parsed to determine:
  - Start and end year-month (for filtering by date)
  - Town name (for filtering by location)
- The dataset must be located at the path specified in `Constant.FILEPATH`
```
