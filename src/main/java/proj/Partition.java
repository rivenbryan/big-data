package proj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Partition class represents a collection of blocks grouped by a specific key (e.g., column value).
 * Used to organize data into partitions for efficient scanning and filtering.
 */
public class Partition {

    private Map<String, List<Block<?>>> blockHashMap;

    /**
     * Constructs an empty Partition with no initial blocks.
     */
    public Partition() {
        this.blockHashMap = new HashMap<>();
    }

    /**
     * Adds a block to the partition under the specified key.
     *
     * @param key   The partition key
     * @param block The block to add
     */
    public void add(String key, Block<?> block) {
        blockHashMap.computeIfAbsent(key, k -> new ArrayList<>()).add(block);
    }

    /**
     * Retrieves the list of blocks associated with the given key.
     *
     * @param key The partition key
     * @return List of blocks for the key, or null if none exist
     */
    public List<Block<?>> get(String key) {
        return blockHashMap.get(key);
    }

    /**
     * Returns the entire map of partition keys to their corresponding block lists.
     *
     * @return The internal block hash map
     */
    public Map<String, List<Block<?>>> getBlockHashMap() {
        return blockHashMap;
    }
}
