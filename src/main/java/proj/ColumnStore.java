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

    public ColumnStore() {
        this.disk = new Disk();
        this.indexSet = new HashSet<>();
        this.partitionBy = null;
    }

    public ColumnStore(String partitionBy) {
        this.disk = new Disk();
        this.indexSet = new HashSet<>();
        this.partitionBy = partitionBy;
    }

    public ColumnStore(ColumnStore other) {
        this.disk = other.disk;
        this.indexSet = new HashSet<>(other.indexSet);
        this.headers = Arrays.copyOf(other.headers, other.headers.length);
        this.partitionBy = other.partitionBy;
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
                    currentBlocks.put(header.trim(), new Block<>());
                }
            }

            String line;
            while ((line = br.readLine()) != null) {
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
                            Block<Object> newBlock = new Block<>();
                            newBlock.addData(value, dataSize);
                            partition.add(columnName, newBlock);
                        } else {
                            ((Block<Object>) blocks.get(blocks.size() - 1)).addData(value, dataSize);
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
                            block = new Block<>();
                            currentBlocks.put(columnName, block);
                        }
                        block.addData(value, dataSize);
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
            List<Object> columnValues = flattenColumn(columnName);
            indexSet.removeIf(i -> {
                Object value = columnValues.get(i);
                return !valueToMatch.equals(value.toString());
            });
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

    public void filterDataByRange(String columnName, String operator, float valueToMatch) {
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

            switch (operator) {
                case ">=": return !(value >= valueToMatch);
                case "<=": return !(value <= valueToMatch);
                case ">": return !(value > valueToMatch);
                case "<": return !(value < valueToMatch);
                default: throw new IllegalArgumentException("Invalid operator: " + operator);
            }
        });
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
