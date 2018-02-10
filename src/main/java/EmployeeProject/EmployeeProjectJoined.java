package EmployeeProject;

import hbase.HBaseRecord;
import hbase.HBaseTable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import projects.Project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class EmployeeProjectJoined extends HBaseTable {

    private String familyA = "Identity";
    private String familyB = "Details";

    private String qualifierA1 = "ProjectId";
    private String qualifierA2 = "Name";
    private String qualifierA3 = "Description";
    private String qualifierA4 = "Location";

    private String qualifierB1 = "EmployeeId";
    private String qualifierB2 = "Specialty";
    private String qualifierB3 = "Sex";

    public EmployeeProjectJoined(String tableName) {
        super(tableName);
        createTable(tableName);
    }

    public String getFamilyA() { return familyA; }
    public void setFamilyA(String familyA) { this.familyA = familyA; }
    public String getFamilyB() { return familyB; }
    public void setFamilyB(String familyB) { this.familyB = familyB; }
    public String getQualifierA1() { return qualifierA1; }
    public void setQualifierA1(String qualifierA1) { this.qualifierA1 = qualifierA1; }
    public String getQualifierA2() { return qualifierA2; }
    public void setQualifierA2(String qualifierA2) { this.qualifierA2 = qualifierA2; }
    public String getQualifierA3() { return qualifierA3; }
    public void setQualifierA3(String qualifierA3) { this.qualifierA3 = qualifierA3; }
    public String getQualifierA4() { return qualifierA4; }
    public void setQualifierA4(String qualifierA4) { this.qualifierA4 = qualifierA4; }
    public String getQualifierB1() { return qualifierB1; }
    public void setQualifierB1(String qualifierB1) { this.qualifierB1 = qualifierB1; }
    public String getQualifierB2() { return qualifierB2; }
    public void setQualifierB2(String qualifierB2) { this.qualifierB2 = qualifierB2; }
    public String getQualifierB3() { return qualifierB3; }
    public void setQualifierB3(String qualifierB3) { this.qualifierB3 = qualifierB3; }


    public boolean insertRecord(HBaseRecord record) {
        manager.addRecordToTable(name, record.toPutObject());
        return true;
    }

    @Override
    public void createTable(String name) {
        List<String> families = new ArrayList<>();
        families.add(familyA);
        families.add(familyB);
        manager.createTable(name, families);
    }

    @Override
    public HashMap<String, HashSet<String>> getStructure() {
        HashSet<String> columnFamilyA = new HashSet<>();
        columnFamilyA.add(qualifierA1);
        columnFamilyA.add(qualifierA2);
        columnFamilyA.add(qualifierA3);
        columnFamilyA.add(qualifierA4);

        HashSet<String> columnFamilyB = new HashSet<>();
        columnFamilyA.add(qualifierB1);
        columnFamilyA.add(qualifierB2);
        columnFamilyA.add(qualifierB3);

        HashMap<String, HashSet<String>> structure = new HashMap<>();
        structure.put(familyA, columnFamilyA);
        structure.put(familyB, columnFamilyB);

        return structure;
    }

    // Not needed in this example
    public HashMap<Integer, HashSet<HBaseRecord>> loadTableInHashMap() { return null; }
    public int findHashKey(int id) { return 0; }
}

