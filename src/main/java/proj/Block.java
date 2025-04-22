package proj;

import java.util.ArrayList;
import java.util.List;

/**
 * Block class represents a fixed-size container used to store columnar data entries.
 * Optionally supports zone map indexing to enable block-level filtering.
 *
 * @param <T> The data type stored in this block (must be Comparable if using zone maps).
 */
public class Block<T> {

    private int remainingSize;
    private List<T> dataList;

    private T min;
    private T max;
    private boolean useZoneMap;
    
    /**
     * Default constructor that creates a block without zone map indexing.
     */
    public Block() {
        this(false);
    }

    /**
     * Constructor that allows enabling zone map indexing.
     *
     * @param useZoneMap Whether to enable zone map indexing on this block
     */
    public Block(boolean useZoneMap) {
        this.dataList = new ArrayList<>();
        this.remainingSize = Constant.DEFAULT_BLOCK_SIZE;
        this.useZoneMap = useZoneMap;
    }

    /**
     * Adds a new data item to the block and updates zone map if enabled.
     *
     * @param data The data item to add
     * @param dataSize The size of the data item (used to update remaining size)
     */
    public void addData(T data, int dataSize) {
        dataList.add(data);
        remainingSize -= dataSize;

        if (useZoneMap && data instanceof Number && data instanceof Comparable<?>) {
            try {
                @SuppressWarnings("unchecked")
                Comparable<T> comp = (Comparable<T>) data;
                if (min == null || comp.compareTo(min) < 0) min = data;
                if (max == null || comp.compareTo(max) > 0) max = data;
            } catch (ClassCastException e) {
                System.err.println("ClassCastException while comparing zone map values.");
            }
        }
    }

    /**
     * Checks whether the block has enough space to accommodate data of the given size.
     *
     * @param dataSize The size of the incoming data item
     * @return True if data can be added, false otherwise
     */
    public boolean isAbleToAdd(int dataSize) {
        return dataSize <= remainingSize;
    }

    /**
     * Retrieves the list of data stored in this block.
     *
     * @return List of stored data items
     */
    public List<T> getDataList() {
        return dataList;
    }

    /**
     * Gets the minimum value in the block (used for zone map indexing).
     *
     * @return Minimum value or null if not available
     */
    public T getMin() {
        return min;
    }

    /**
     * Gets the maximum value in the block (used for zone map indexing).
     *
     * @return Maximum value or null if not available
     */
    public T getMax() {
        return max;
    }

    /**
     * Checks if the given value falls within the zone map boundaries of this block.
     *
     * @param value The value to check
     * @return True if zone map is disabled or value falls within min and max bounds
     */
    public boolean isInZone(Object value) {
        if (!useZoneMap) return true;
        if (value == null || min == null || max == null) return true;
        if (!(value instanceof Comparable)) return true;

        try {
            @SuppressWarnings("unchecked")
            Comparable<Object> comp = (Comparable<Object>) value;
            return comp.compareTo(min) >= 0 && comp.compareTo(max) <= 0;
        } catch (ClassCastException e) {
            return true; 
        }
    }
}