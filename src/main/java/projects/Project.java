package projects;

import hbase.HBaseRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Project extends HBaseRecord {

    private int id;
    private String name;
    private String description;
    private String location;

    public Project(Projects belongs, int id, String name, String description, String location) {
        belongsTo = belongs;
        this.id = id;
        this.name = name;
        this.description = description;
        this.location = location;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public Put toPutObject() {
        Projects proj = (Projects) belongsTo;
        Put p = new Put(Bytes.toBytes(id));
        p.addImmutable(proj.getFamilyA().getBytes(), proj.getQualifierA1().getBytes(), Bytes.toBytes(String.valueOf(id)));
        p.addImmutable(proj.getFamilyA().getBytes(), proj.getQualifierA2().getBytes(), Bytes.toBytes(name));
        p.addImmutable(proj.getFamilyB().getBytes(), proj.getQualifierB1().getBytes(), Bytes.toBytes(description));
        p.addImmutable(proj.getFamilyB().getBytes(), proj.getQualifierB2().getBytes(), Bytes.toBytes(location));
        return p;
    }
}
