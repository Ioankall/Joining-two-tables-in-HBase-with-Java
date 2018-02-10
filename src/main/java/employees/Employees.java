package employees;

import hbase.HBaseRecord;
import hbase.HBaseTable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Employees extends HBaseTable {

    private static final int NUMBER_OF_BACKETS_IN_HASHTABLE = 5;

    private String familyA = "Identity";
    private String familyB = "Details";

    private String qualifierA1 = "EmployeeId";
    private String qualifierA2 = "ProjectId";
    private String qualifierB1 = "Specialty";
    private String qualifierB2 = "Sex";

    public Employees(String tableName) {
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
    public String getQualifierB1() { return qualifierB1; }
    public void setQualifierB1(String qualifierB1) { this.qualifierB1 = qualifierB1; }
    public String getQualifierB2() { return qualifierB2; }
    public void setQualifierB2(String qualifierB2) { this.qualifierB2 = qualifierB2; }

    public Employees() {

    }

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

        HashSet<String> columnFamilyB = new HashSet<>();
        columnFamilyA.add(qualifierB1);
        columnFamilyA.add(qualifierB2);

        HashMap<String, HashSet<String>> structure = new HashMap<>();
        structure.put(familyA, columnFamilyA);
        structure.put(familyB, columnFamilyB);

        return structure;
    }

    public HashMap<Integer, HashSet<HBaseRecord>> loadTableInHashMap() {
        HashMap<Integer, HashSet<HBaseRecord>> loadedContent = new HashMap<>();

        Table table = manager.getTable(this.name);
        try {
            ResultScanner scanner = table.getScanner(new Scan());

            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                Result row = table.get(new Get(rr.getRow()));
                Employee emp = new Employee(this,
                        Integer.valueOf(new String(CellUtil.cloneValue(row.getColumnCells(familyA.getBytes(), qualifierA1.getBytes()).get(0)))),
                        Integer.valueOf(new String(CellUtil.cloneValue(row.getColumnCells(familyA.getBytes(), qualifierA2.getBytes()).get(0)))),
                        new String(CellUtil.cloneValue(row.getColumnCells(familyB.getBytes(), qualifierB1.getBytes()).get(0))),
                        new String(CellUtil.cloneValue(row.getColumnCells(familyB.getBytes(), qualifierB2.getBytes()).get(0))));

                int position = findHashKey(emp.getId());
                HashSet<HBaseRecord> set = loadedContent.containsKey(position) ?
                        loadedContent.get(Integer.valueOf(position)) : new HashSet<>();
                set.add(emp);
                loadedContent.put(position, set);
            }

            return loadedContent;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int findHashKey(int id) {
        return Integer.valueOf(id % NUMBER_OF_BACKETS_IN_HASHTABLE);
    }

}
