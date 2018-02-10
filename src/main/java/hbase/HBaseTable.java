package hbase;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public abstract class HBaseTable implements DatabaseTable {

    protected String name;

    protected static HBaseManager manager;

    public HBaseTable() {
        manager = HBaseManager.getInstance();
    }

    public HBaseTable(String tableName) {
        manager = HBaseManager.getInstance();
        name = tableName;
    }

    public String getName() {
        return name;
    }

    public boolean isEmpty() {
        Table table = manager.getTable(name);
        try {
            ResultScanner scanner = table.getScanner(new Scan());
            if (scanner.next() != null)
                return false;

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
