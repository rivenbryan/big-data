package proj;

import java.util.ArrayList;
import java.util.List;

public class Block<T> {
    private int remainingSize;
    private List<T> dataList;
    private T min;
    private T max;
    private boolean useZoneMap;
    public Block() {
        this(false);
    }

    public Block(boolean useZoneMap) {
        this.dataList = new ArrayList<>();
        this.remainingSize = Constant.DEFAULT_BLOCK_SIZE;
        this.useZoneMap = useZoneMap;
    }

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

    public boolean isAbleToAdd(int dataSize) {
        return dataSize <= remainingSize;
    }

    public List<T> getDataList() {
        return dataList;
    }

    public T getMin() {
        return min;
    }

    public T getMax() {
        return max;
    }

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
