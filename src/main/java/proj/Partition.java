package proj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {
    private Map<String, List<Block<?>>> blockHashMap;

    public Partition() {
        this.blockHashMap = new HashMap<>();
    }

    public void add(String key, Block<?> block) {
    	blockHashMap.computeIfAbsent(key, k -> new ArrayList<>()).add(block);
    }

    public List<Block<?>> get(String key) {
        return blockHashMap.get(key);
    }
}