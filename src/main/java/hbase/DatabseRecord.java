package hbase;

import org.apache.hadoop.hbase.client.Put;

public interface DatabseRecord {

    Put toPutObject();
}
