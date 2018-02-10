package EmployeeProject;

import hbase.HBaseRecord;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class EmployeeProject extends HBaseRecord {

    private int id;
    private int employeeId;
    private String employeeSpecialty;
    private String employeeSex;
    private int projectId;
    private String projectName;
    private String description;
    private String location;


    public EmployeeProject(EmployeeProjectJoined belongs, int id, int employeeId, int projectId, String employeeSpecialty, String employeeSex,
                           String projectName, String description, String location) {
        belongsTo = belongs;
        this.id = id;
        this.employeeId = employeeId;
        this.projectId = projectId;
        this.employeeSpecialty = employeeSpecialty;
        this.employeeSex = employeeSex;
        this.projectName = projectName;
        this.description = description;
        this.location = location;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public int getEmployeeId() { return employeeId; }
    public void setEmployeeId(int employeeId) { this.employeeId = employeeId; }
    public String getEmployeeSpecialty() { return employeeSpecialty; }
    public void setEmployeeSpecialty(String employeeSpecialty) { this.employeeSpecialty = employeeSpecialty; }
    public String getEmployeeSex() { return employeeSex; }
    public void setEmployeeSex(String employeeSex) { this.employeeSex = employeeSex; }
    public int getProjectId() { return projectId; }
    public void setProjectId(int projectId) { this.projectId = projectId; }
    public String getProjectName() { return projectName; }
    public void setProjectName(String projectName) { this.projectName = projectName; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public Put toPutObject() {
        EmployeeProjectJoined joined = (EmployeeProjectJoined) belongsTo;
        Put p = new Put(Bytes.toBytes(id));
        p.addImmutable(joined.getFamilyA().getBytes(), joined.getQualifierB1().getBytes(), Bytes.toBytes(String.valueOf(employeeId)));
        p.addImmutable(joined.getFamilyB().getBytes(), joined.getQualifierA1().getBytes(), Bytes.toBytes(String.valueOf(projectId)));
        p.addImmutable(joined.getFamilyA().getBytes(), joined.getQualifierB2().getBytes(), Bytes.toBytes(employeeSpecialty));
        p.addImmutable(joined.getFamilyA().getBytes(), joined.getQualifierB3().getBytes(), Bytes.toBytes(employeeSex));
        p.addImmutable(joined.getFamilyB().getBytes(), joined.getQualifierA2().getBytes(), Bytes.toBytes(projectName));
        p.addImmutable(joined.getFamilyB().getBytes(), joined.getQualifierA3().getBytes(), Bytes.toBytes(description));
        p.addImmutable(joined.getFamilyB().getBytes(), joined.getQualifierA4().getBytes(), Bytes.toBytes(location));
        return p;
    }
}
