package projectDDM;

import EmployeeProject.EmployeeProject;
import EmployeeProject.EmployeeProjectJoined;
import employees.Employee;
import employees.Employees;
import hbase.HBaseManager;
import hbase.HBaseRecord;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import projects.Project;
import projects.Projects;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ProjectDDM {

    private static final int NUMBER_OF_PROJECTS = 10;
    private static final int NUMBER_OF_EMPLOYEES_PER_PROJECT = 3;

    private static HBaseManager manager;

    private static Projects projects;
    private static Employees employees;
    private static EmployeeProjectJoined join;

    private static int joinedCounter = 0;
    private static HashMap<Integer, HashSet<HBaseRecord>> projectsMap;

    public static void main (String [] args) throws IOException {
        manager = HBaseManager.getInstance();

        setUpDummyTables();

        projectsMap = projects.loadTableInHashMap();
        singlePassJoin();
    }

    private static void singlePassJoin() {
        Table table = manager.getTable(employees.getName());

        try {
            ResultScanner scanner = table.getScanner(new Scan());
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                Result row = table.get(new Get(rr.getRow()));

                Employee emp = new Employee(employees,
                        Integer.valueOf(Bytes.toString(row.getValue(employees.getFamilyA().getBytes(), employees.getQualifierA1().getBytes()))),
                        Integer.valueOf(Bytes.toString(row.getValue(employees.getFamilyA().getBytes(), employees.getQualifierA2().getBytes()))),
                        Bytes.toString(row.getValue(employees.getFamilyB().getBytes(), employees.getQualifierB1().getBytes())),
                        Bytes.toString(row.getValue(employees.getFamilyB().getBytes(), employees.getQualifierB2().getBytes())));

                List<HBaseRecord> projects = findMatchingProjectsForEmployee(emp);

                projects.forEach(project -> join(emp, ((Project) project)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void join(Employee emp, Project proj) {
        EmployeeProject obj = new EmployeeProject(join,
                joinedCounter,
                emp.getId(),
                proj.getId(),
                emp.getSpecialty(),
                emp.getSex(),
                proj.getName(),
                proj.getDescription(),
                proj.getLocation()
                );

        join.insertRecord(obj);
        joinedCounter++;
    }

    private static List<HBaseRecord> findMatchingProjectsForEmployee(Employee employee) {
        int key = projects.findHashKey(employee.getProjectId());

        return projectsMap.get(key).stream()
                .filter(project -> ((Project) project).getId() == employee.getProjectId())
                .collect(Collectors.toList());
    }

    private static void setUpDummyTables() throws IOException {
        initTables();
        addDummyProjects();
        addDummyEmployees();
    }

    private static void initTables() throws IOException {
        projects = new Projects("Projects");
        employees = new Employees("Employees");
        join = new EmployeeProjectJoined("JoinResult");
    }

    private static void addDummyProjects() throws IOException {
        if (!projects.isEmpty())
            return;

        for (int i = 0; i < NUMBER_OF_PROJECTS; i++) {
            projects.insertRecord(new Project(projects, i, "Project_" + String.valueOf(i),
                    "Description for project_" + String.valueOf(i), "Location of project_" + String.valueOf(i)));
        }
    }

    private static void addDummyEmployees() throws IOException {
        if (!employees.isEmpty())
            return;

        for (int i = 0; i < NUMBER_OF_PROJECTS; i++) {
            for (int j = 0; j < NUMBER_OF_EMPLOYEES_PER_PROJECT; j++) {
                employees.insertRecord(new Employee(employees, (i * NUMBER_OF_EMPLOYEES_PER_PROJECT + j), i,
                        "Specialty_" + String.valueOf(i), "Male"));
            }
        }
    }
}
