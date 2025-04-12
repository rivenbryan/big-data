package proj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Disk {
    private Map<String, List<?>> dataHashMap;

    public Disk() {
        this.dataHashMap = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public <T> void add(String key, T value) {
        dataHashMap.computeIfAbsent(key, k -> new ArrayList<T>());
        List<T> list = (List<T>) dataHashMap.get(key); 
        list.add(value);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> get(String key) {
        return (List<T>) dataHashMap.get(key);
    }
    
    public Map<String, List<?>> getAll() {
        return dataHashMap;
    }
}
