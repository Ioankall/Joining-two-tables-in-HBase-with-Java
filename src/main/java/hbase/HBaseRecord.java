package hbase;

public abstract class HBaseRecord implements DatabseRecord {

    protected HBaseTable belongsTo;
    protected int recordId;

    public int getRecordId() { return recordId; }
    public void setRecordId(int recordId) { this.recordId = recordId; }

}
