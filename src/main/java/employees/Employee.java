package employees;

import hbase.HBaseRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import projects.Projects;

public class Employee extends HBaseRecord {

    private int id;
    private int projectId;
    private String specialty;
    private String sex;

    public Employee(Employees belongs, int id, int projectId, String specialty, String sex) {
        belongsTo = belongs;
        this.id = id;
        this.projectId = projectId;
        this.specialty = specialty;
        this.sex = sex;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public int getProjectId() { return projectId; }
    public void setProjectId(int projectId) { this.projectId = projectId; }
    public String getSpecialty() { return specialty; }
    public void setSpecialty(String specialty) { this.specialty = specialty; }
    public String getSex() { return sex; }
    public void setSex(String sex) { this.sex = sex; }

    public Put toPutObject() {
        Employees emp = (Employees) belongsTo;
        Put p = new Put(Bytes.toBytes(id));
        p.addImmutable(emp.getFamilyA().getBytes(), emp.getQualifierA1().getBytes(), Bytes.toBytes(String.valueOf(id)));
        p.addImmutable(emp.getFamilyA().getBytes(), emp.getQualifierA2().getBytes(), Bytes.toBytes(String.valueOf(projectId)));
        p.addImmutable(emp.getFamilyB().getBytes(), emp.getQualifierB1().getBytes(), Bytes.toBytes(specialty));
        p.addImmutable(emp.getFamilyB().getBytes(), emp.getQualifierB2().getBytes(), Bytes.toBytes(sex));
        return p;
    }
}
