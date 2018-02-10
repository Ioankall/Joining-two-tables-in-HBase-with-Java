package hbase;

import java.util.HashMap;
import java.util.HashSet;

public interface DatabaseTable {

    void createTable(String name);

    boolean insertRecord(HBaseRecord record);

    boolean isEmpty();

    HashMap<String, HashSet<String>> getStructure();

    HashMap<Integer, HashSet<HBaseRecord>> loadTableInHashMap();

    int findHashKey(int id);

}
