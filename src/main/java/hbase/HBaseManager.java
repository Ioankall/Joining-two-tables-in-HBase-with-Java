package hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HBaseManager {

    private static final String HBASE_SITE_PATH = "/home/user/bdc/hbase-1.0.1.1/conf/hbase-site.xml";

    private static HBaseManager instance = null;

    private static Configuration config;
    private static Connection connection;
    private static Admin admin;

    protected HBaseManager() {}

    public static HBaseManager getInstance() {
        if(instance == null) {
            instance = new HBaseManager();
            initializeConnection();
        }
        return instance;
    }

    private static boolean initializeConnection() {
        config = HBaseConfiguration.create();
        config.addResource(new Path(HBASE_SITE_PATH));

        try {
            HBaseAdmin.checkHBaseAvailable(config);

            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (ServiceException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public Table getTable(String tableName) {

        TableName name = TableName.valueOf(tableName);

        try {
            return connection.getTable(name);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Table createTable (String tableName, List<String> columnFamilies) {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        columnFamilies.stream().forEach(cFamily -> descriptor.addFamily(new HColumnDescriptor(cFamily)));

        try {
            if (tableExist(tableName)) return getTable(tableName);
            admin.createTable(descriptor);
            return getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    public boolean tableExist(String tableName) {
        TableName name = TableName.valueOf(tableName);
        try {
            return admin.tableExists(name);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean deleteTableIfExist(String tableName) {
        TableName name = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(name)) {
                admin.disableTable(name);
                admin.deleteTable(name);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean addRecordToTable(String tableName, Put record) {
        Table table = getTable(tableName);
        try {
            table.put(record);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
