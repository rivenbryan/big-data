package proj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Disk class serves as a simple in-memory storage system that maps column names to their respective data lists.
 * Data map will contain the mapping for either Block or Partition 
 * Acts as the underlying storage layer for the columnar data model.
 */
public class Disk {

    private Map<String, List<?>> dataHashMap;

    /**
     * Constructs a new Disk instance with an empty data map. 
     */
    public Disk() {
        this.dataHashMap = new HashMap<>();
    }

    /**
     * Adds a value to the data list corresponding to the given key (column name).
     *
     * @param key   The column name
     * @param value The data value to add
     * @param <T>   The data type of the value
     */
    @SuppressWarnings("unchecked")
    public <T> void add(String key, T value) {
        dataHashMap.computeIfAbsent(key, k -> new ArrayList<T>());
        List<T> list = (List<T>) dataHashMap.get(key); 
        list.add(value);
    }

    /**
     * Retrieves the data list for the specified column key.
     *
     * @param key The column name
     * @param <T> The expected data type
     * @return A list of values for the column, or null if the key does not exist
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> get(String key) {
        return (List<T>) dataHashMap.get(key);
    }

    /**
     * Returns the entire internal data map.
     *
     * @return A map of column names to their respective data lists
     */
    public Map<String, List<?>> getAll() {
        return dataHashMap;
    }
}
