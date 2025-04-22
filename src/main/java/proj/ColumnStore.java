package proj;

import java.io.*;
import java.util.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ColumnStore {

    private final Disk disk;
    private Set<Integer> indexSet;
    private String[] headers;
    private String partitionBy;
    private String zoneMapBy;
    private Map<Block<?>, int[]> blockIndexMap;

    public ColumnStore() {
        this.disk = new Disk();
        this.indexSet = new HashSet<>();
        this.partitionBy = null;
        this.zoneMapBy = null;
        this.blockIndexMap = new HashMap<>();
    }

    public ColumnStore(String partitionBy) {
        this.disk = new Disk();
        this.indexSet = new HashSet<>();
        this.partitionBy = partitionBy;
        this.zoneMapBy = null;
        this.blockIndexMap = new HashMap<>();
    }

    public ColumnStore(String partitionBy, String zoneMapBy) {
        this.disk = new Disk();
        this.indexSet = new HashSet<>();
        this.partitionBy = partitionBy;
        this.zoneMapBy = zoneMapBy;
        this.blockIndexMap = new HashMap<>();
    }

    public ColumnStore(ColumnStore other) {
        this.disk = other.disk;
        this.indexSet = new HashSet<>(other.indexSet);
        this.headers = Arrays.copyOf(other.headers, other.headers.length);
        this.partitionBy = other.partitionBy;
        this.zoneMapBy = other.zoneMapBy;
        this.blockIndexMap = new HashMap<>(other.blockIndexMap);
    }

    @SuppressWarnings("unchecked")
    public void loadData(String filepath) throws IOException {
        long startTime = System.currentTimeMillis();
        int lineCount = 0;
        InputStream input = getClass().getClassLoader().getResourceAsStream(filepath);

        if (input == null) {
            throw new FileNotFoundException("File not found: " + filepath);
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(input))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("Empty CSV file!");
            }

            headers = headerLine.split(",");
            int columnCount = headers.length;

            Map<String, Integer> headerIndexMap = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                headerIndexMap.put(headers[i].trim(), i);
            }

            if (partitionBy != null && !headerIndexMap.containsKey(partitionBy)) {
                throw new IllegalArgumentException("Partition column not found: " + partitionBy);
            }

            Map<String, Partition> partitions = new HashMap<>();
            Map<String, Block<Object>> currentBlocks = new HashMap<>();

            if (partitionBy == null) {
                for (String header : headers) {
                    boolean useZoneMap = zoneMapBy != null && zoneMapBy.equals(header.trim());
                    currentBlocks.put(header.trim(), new Block<>(useZoneMap));
                }
            }

            String line;
            while ((line = br.readLine()) != null) {
                final int currentLine = lineCount;
                String[] values = line.split(",", -1);
                if (values.length != columnCount) continue;
                
                

                if (partitionBy != null) {
                    String partitionKey = values[headerIndexMap.get(partitionBy)].trim();
                    Partition partition = partitions.computeIfAbsent(partitionKey, k -> new Partition());

                    for (int i = 0; i < columnCount; i++) {
                        String columnName = headers[i].trim();
                        String rawValue = values[i].trim();

                        Object value;
                        int dataSize;
                        try {
                            value = Float.parseFloat(rawValue);
                            dataSize = Constant.FLOAT_BYTES;
                        } catch (NumberFormatException e) {
                            value = rawValue;
                            dataSize = Constant.VARCHAR_BYTES;
                        }

                        List<Block<?>> blocks = partition.get(columnName);
                        if (blocks == null || blocks.isEmpty() || !blocks.get(blocks.size() - 1).isAbleToAdd(dataSize)) {
                            Block<Object> newBlock = new Block<>(zoneMapBy != null && zoneMapBy.equals(columnName));
                            newBlock.addData(value, dataSize);
                            partition.add(columnName, newBlock);

                            if (zoneMapBy != null && zoneMapBy.equals(columnName)) {
                                blockIndexMap.put(newBlock, new int[]{currentLine, currentLine});
                            }
                        } else {
                            Block<Object> block = (Block<Object>) blocks.get(blocks.size() - 1);
                            block.addData(value, dataSize);

                            if (zoneMapBy != null && zoneMapBy.equals(columnName)) {
                                blockIndexMap.compute(block, (k, v) -> {
                                    if (v == null) return new int[]{currentLine, currentLine};
                                    v[1] = currentLine;
                                    return v;
                                });
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < columnCount; i++) {
                        String columnName = headers[i].trim();
                        String rawValue = values[i].trim();

                        Object value;
                        int dataSize;
                        try {
                            value = Float.parseFloat(rawValue);
                            dataSize = Constant.FLOAT_BYTES;
                        } catch (NumberFormatException e) {
                            value = rawValue;
                            dataSize = Constant.VARCHAR_BYTES;
                        }

                        Block<Object> block = currentBlocks.get(columnName);
                        if (!block.isAbleToAdd(dataSize)) {
                            disk.add(columnName, block);
                            block = new Block<>(zoneMapBy != null && zoneMapBy.equals(columnName));
                            currentBlocks.put(columnName, block);
                        }
                        block.addData(value, dataSize);

                        if (zoneMapBy != null && zoneMapBy.equals(columnName)) {
                            final Block<Object> currentBlock = block;
                            blockIndexMap.compute(currentBlock, (k, v) -> {
                                if (v == null) return new int[]{currentLine, currentLine};
                                v[1] = currentLine;
                                return v;
                            });
                        }
                    }
                }

                indexSet.add(lineCount);
                lineCount++;
            }

            if (partitionBy != null) {
                for (Map.Entry<String, Partition> entry : partitions.entrySet()) {
                    disk.add(entry.getKey(), entry.getValue());
                }
            } else {
                for (Map.Entry<String, Block<Object>> entry : currentBlocks.entrySet()) {
                    disk.add(entry.getKey(), entry.getValue());
                }
            }

            System.out.println("\n===== Disk Contents After Loading =====");
            for (Map.Entry<String, List<?>> entry : disk.getAll().entrySet()) {
                String key = entry.getKey();
                List<?> value = entry.getValue();

                if (!value.isEmpty()) {
                    Object first = value.get(0);
                    if (first instanceof Partition) {
                        System.out.println("Key = " + key + " -> Partition");
                    } else if (first instanceof Block) {
                        System.out.println("Key = " + key + " -> List<Block>");
                    } else {
                        System.out.println("Key = " + key + " -> Unknown Type");
                    }
                } else {
                    System.out.println("Key = " + key + " -> Empty List");
                }
            }
            System.out.println("========================================\n");

            if (partitionBy == null) {
                System.out.println("===== Block Count Per Column =====");
                for (Map.Entry<String, List<?>> entry : disk.getAll().entrySet()) {
                    String column = entry.getKey();
                    List<?> blocks = entry.getValue();
                    if (!blocks.isEmpty() && blocks.get(0) instanceof Block<?>) {
                        System.out.println("Column: " + column + " -> Blocks Allocated: " + blocks.size());
                    }
                }
                System.out.println("==================================\n");
            }

            if (zoneMapBy != null) {
                System.out.println("===== Block Index Range (Zone Map) =====");
                for (Map.Entry<Block<?>, int[]> entry : blockIndexMap.entrySet()) {
                    Block<?> block = entry.getKey();
                    int[] range = entry.getValue();
                    System.out.println("Block Min: " + block.getMin() + ", Max: " + block.getMax() +
                                       " -> Index Range: [" + range[0] + ", " + range[1] + "]");
                }
                System.out.println("========================================");
            }
        }

        long endTime = System.currentTimeMillis();
        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        System.out.println(String.format("Data loading completed in %.2f seconds.", totalTimeSeconds));
    }

    public void filterDataByEquality(String columnName, String valueToMatch) {
        if (partitionBy != null && partitionBy.equals(columnName)) {
            Map<String, List<?>> rawPartitions = disk.getAll();
            Set<String> toRemove = new HashSet<>();
            for (String partitionKey : rawPartitions.keySet()) {
                if (!partitionKey.equals(valueToMatch)) {
                    toRemove.add(partitionKey);
                }
            }
            for (String key : toRemove) {
                rawPartitions.remove(key);
            }
            rebuildIndexSetFromPartitions();
        } else {
            if (zoneMapBy != null && zoneMapBy.equals(columnName)) {
                Set<Integer> candidateIndices = new HashSet<>();

                for (Map.Entry<Block<?>, int[]> entry : blockIndexMap.entrySet()) {
                    Block<?> block = entry.getKey();
                    int[] range = entry.getValue(); 

                    String minStr = String.valueOf(block.getMin());
                    String maxStr = String.valueOf(block.getMax());

                    if (valueToMatch.compareTo(minStr) >= 0 && valueToMatch.compareTo(maxStr) <= 0) {
                        for (int i = range[0]; i <= range[1]; i++) {
                            candidateIndices.add(i);
                        }
                    }
                }
                indexSet.retainAll(candidateIndices);
            }

            List<Object> columnValues = flattenColumn(columnName);
            indexSet.removeIf(i -> {
                Object value = columnValues.get(i);
                return !valueToMatch.equals(value.toString());
            });
        }
    }
    
    public void filterDataByRange(String columnName, String operator, float valueToMatch) {
        if (zoneMapBy != null && zoneMapBy.equals(columnName)) {
            Set<Integer> candidateIndices = new HashSet<>();

            for (Map.Entry<Block<?>, int[]> entry : blockIndexMap.entrySet()) {
                Block<?> block = entry.getKey();
                int[] range = entry.getValue();

                Float min = tryParseFloat(block.getMin());
                Float max = tryParseFloat(block.getMax());
                if (min == null || max == null) continue;

                boolean isMatch = switch (operator) {
                    case ">=" -> max >= valueToMatch;
                    case "<=" -> min <= valueToMatch;
                    case ">" -> max > valueToMatch;
                    case "<" -> min < valueToMatch;
                    default -> throw new IllegalArgumentException("Invalid operator: " + operator);
                };

                if (isMatch) {
                    for (int i = range[0]; i <= range[1]; i++) {
                        candidateIndices.add(i);
                    }
                }
            }
            indexSet.retainAll(candidateIndices);
        }

        List<Object> columnValues = flattenColumn(columnName);
        indexSet.removeIf(i -> {
            Object valueObj = columnValues.get(i);
            if (valueObj == null) return true;
            float value;
            try {
                value = Float.parseFloat(valueObj.toString());
            } catch (NumberFormatException e) {
                return true;
            }

            return switch (operator) {
                case ">=" -> !(value >= valueToMatch);
                case "<=" -> !(value <= valueToMatch);
                case ">" -> !(value > valueToMatch);
                case "<" -> !(value < valueToMatch);
                default -> throw new IllegalArgumentException("Invalid operator: " + operator);
            };
        });
    }
    
    private Float tryParseFloat(Object obj) {
        if (obj == null) return null;
        try {
            return Float.parseFloat(obj.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public void filterDataByDateRange(String columnName, String startDate, String endDate) {
        if (partitionBy != null && partitionBy.equals(columnName)) {
            Map<String, List<?>> rawPartitions = disk.getAll();
            LocalDate start = parseToLocalDate(startDate);
            LocalDate end = parseToLocalDate(endDate);

            Set<String> toRemove = new HashSet<>();
            for (String partitionKey : rawPartitions.keySet()) {
                try {
                    LocalDate partitionDate = parseToLocalDate(partitionKey);
                    if (partitionDate.isBefore(start) || partitionDate.isAfter(end)) {
                        toRemove.add(partitionKey);
                    }
                } catch (DateTimeParseException e) {
                    toRemove.add(partitionKey);
                }
            }
            for (String key : toRemove) {
                rawPartitions.remove(key);
            }
            rebuildIndexSetFromPartitions();
        } else {
            List<Object> columnValues = flattenColumn(columnName);
            LocalDate start = parseToLocalDate(startDate);
            LocalDate end = parseToLocalDate(endDate);

            indexSet.removeIf(i -> {
                String value = columnValues.get(i).toString();
                LocalDate valueDate;
                try {
                    valueDate = parseToLocalDate(value);
                } catch (DateTimeParseException e) {
                    return true;
                }
                return valueDate.isBefore(start) || valueDate.isAfter(end);
            });
        }
    }

    private LocalDate parseToLocalDate(String dateStr) {
        if (dateStr.length() == 4) {
            return LocalDate.parse(dateStr + "-01-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else if (dateStr.length() == 7) {
            return LocalDate.parse(dateStr + "-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            return LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }
    }

    public List<Object> getColumnValues(String columnName) {
        List<Object> columnValues = flattenColumn(columnName);
        List<Object> filteredValues = new ArrayList<>();
        for (Integer idx : indexSet) {
            filteredValues.add(columnValues.get(idx));
        }
        return filteredValues;
    }

    private List<Object> flattenColumn(String columnName) {
        List<Object> result = new ArrayList<>();
        if (partitionBy == null) {
            List<Block<?>> blocks = disk.get(columnName);
            for (Block<?> block : blocks) {
                result.addAll(block.getDataList());
            }
        } else {
            Map<String, List<?>> rawPartitions = disk.getAll();
            for (Map.Entry<String, List<?>> entry : rawPartitions.entrySet()) {
                List<?> partitionList = entry.getValue();
                if (partitionList.isEmpty()) continue;

                Object first = partitionList.get(0);
                if (first instanceof Partition) {
                    Partition partition = (Partition) first;
                    List<Block<?>> blocks = partition.get(columnName);
                    if (blocks != null) {
                        for (Block<?> block : blocks) {
                            result.addAll(block.getDataList());
                        }
                    }
                }
            }
        }
        return result;
    }

    private void rebuildIndexSetFromPartitions() {
        indexSet.clear();
        int currentIndex = 0;
        Map<String, List<?>> rawPartitions = disk.getAll();
        for (Map.Entry<String, List<?>> entry : rawPartitions.entrySet()) {
            List<?> partitionList = entry.getValue();
            if (partitionList.isEmpty()) continue;

            Object first = partitionList.get(0);
            if (first instanceof Partition) {
                Partition partition = (Partition) first;
                for (String columnName : partition.getBlockHashMap().keySet()) {
                    List<Block<?>> blocks = partition.get(columnName);
                    if (blocks != null) {
                        for (Block<?> block : blocks) {
                            for (Object ignored : block.getDataList()) {
                                indexSet.add(currentIndex++);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }
}
