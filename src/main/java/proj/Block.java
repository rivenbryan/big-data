package proj;

import java.util.ArrayList;
import java.util.List;

public class Block<T> {
    private int remainingSize;
    private List<T> dataList;

    public Block() {
        this.dataList = new ArrayList<>();
        this.remainingSize = Constant.DEFAULT_BLOCK_SIZE;
    }
    
    public void addData(T data, int dataSize) {
        dataList.add(data);
        remainingSize -= dataSize;
    }

    public List<T> getDataList() {
        return dataList;
    }

    public boolean isAbleToAdd(int datasize) {
    	return datasize <= remainingSize;
    }
}