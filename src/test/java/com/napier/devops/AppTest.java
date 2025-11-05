package com.napier.devops;

import com.napier.sem.App;
import com.napier.sem.Employee;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class AppTest
{
    static App app;

    @BeforeAll
    static void init()
    {
        app = new App();
    }

    @Test
    void printSalariesTestNull()
    {
        app.printSalaries(null);
    }

    @Test
    void printSalariesTestEmpty()
    {
        ArrayList<Employee> employess = new ArrayList<Employee>();
        app.printSalaries(employess);
    }

    /**
     * @Test
     *
     * This tells JUnit: “Run this method as a test.”

     * void printSalariesTestContainsNull()
     *
     * This is the name of the test. It’s testing what happens when the employee list contains a null.
     *
     * ArrayList<Employee> employess = new ArrayList<Employee>();
     *
     * Creates an empty list of Employee objects.
     *
     * employess.add(null);
     *
     * Adds a null value to the list. This simulates a situation where one of the employees is missing or not properly initialized.*/
    @Test
    void printSalariesTestContainsNull()
    {
        ArrayList<Employee> employess = new ArrayList<Employee>();
        employess.add(null);
        app.printSalaries(employess);
    }


    @Test
    void printSalaries()
    {
        ArrayList<Employee> employees = new ArrayList<Employee>();
        Employee emp = new Employee();
        emp.emp_no = 1;
        emp.first_name = "Kevin";
        emp.last_name = "Chalmers";
        emp.title = "Engineer";
        emp.salary = 55000;
        employees.add(emp);
        app.printSalaries(employees);
    }

    //Test for the display employee class
    @Test
    void printEmployeeTestNull()
    {
        app.displayEmployee(null);
    }

    //Test for empty values
    @Test
    void printEmployeesTestEmpty()
    {
        Employee emp = new Employee();
        app.displayEmployee(emp);
    }

    //Test for null values
    @Test
    void printEmployeesTestContainsNull()
    {
        Employee emp = new Employee();
        app.displayEmployee(null);
        app.displayEmployee(emp);
    }

    //Test for correct values
    @Test
    void printEmployees()
    {
        Employee emp = new Employee();
        emp.emp_no = 255530;
        emp.first_name = "Ronghao";
        emp.last_name = "Garigliano";
        emp.title = "Technique Leader";
        emp.salary = 57499;
        emp.dept_name = "Development";
        emp.manager = "DeForest";
        app.displayEmployee(emp);

    }

}