package proj;

import java.io.*;
import java.util.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class ColumnStore {

    private final Disk disk;
    private final Set<Integer> indexSet = new HashSet<>();
    private String[] headers;

    public ColumnStore() {
        this.disk = new Disk();
    }

    public void loadData(String filepath) throws IOException {
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

            Map<String, Block<Object>> currentBlocks = new HashMap<>();
            for (String header : headers) {
                currentBlocks.put(header.trim(), new Block<>());
            }

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1);

                for (int i = 0; i < columnCount; i++) {
                    String header = headers[i].trim();
                    String rawValue = values.length > i ? values[i].trim() : "";

                    Block<Object> block = currentBlocks.get(header);

                    Object value;
                    int dataSize;

                    try {
                        value = Float.parseFloat(rawValue);
                        dataSize = Constant.FLOAT_BYTES; 
                    } catch (NumberFormatException e) {
                        value = rawValue;
                        dataSize = Constant.VARCHAR_BYTES; 
                    }

                    if (!block.isAbleToAdd(dataSize)) {
                        disk.add(header, block);
                        block = new Block<>();
                        currentBlocks.put(header, block);
                    }
                    block.addData(value, dataSize);
                }

                indexSet.add(lineCount);
                lineCount++;
            }

            for (Map.Entry<String, Block<Object>> entry : currentBlocks.entrySet()) {
                disk.add(entry.getKey(), entry.getValue());
            }
        }
    }

    public void filterDataByEquality(String columnName, String valueToMatch) {
        List<Object> columnValues = flattenColumn(columnName);
        indexSet.removeIf(i -> {
            Object value = columnValues.get(i);
            return !valueToMatch.equals(value.toString());
        });
    }
    
    public void filterDataByDateRange(String columnName, String startDate, String endDate) {
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
                case ">":  return !(value > valueToMatch);
                case "<":  return !(value < valueToMatch);
                default:
                    throw new IllegalArgumentException("Invalid operator: " + operator);
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
        List<Block<?>> blocks = disk.get(columnName);
        List<Object> result = new ArrayList<>();
        for (Block<?> block : blocks) {
            result.addAll(block.getDataList());
        }
        return result;
    }
}
