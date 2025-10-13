package com.napier.sem;

import java.sql.*;

/**
 * This is main application point that connects us to an SQL server to allow us to connect to the correct database.
 * It implements the use of try-catch statements to ensure that the correct database driver is loaded.
 */
public class App
{
    /**
     * Connection to MySQL database.
     */
    private Connection con = null;

    /**
     * The class connect() is used to connect our app MySQL database
     */
    public void connect()
    {
        try
        {
            // Load Database driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Could not load SQL driver");
            System.exit(-1);
        }

        int retries = 10;
        for (int i = 0; i < retries; ++i)
        {
            System.out.println("Connecting to database...");
            try
            {
                // Wait a bit for db to start
                Thread.sleep(30000);
                // Connect to database
                con = DriverManager.getConnection("jdbc:mysql://db:3306/employees?allowPublicKeyRetrieval=true&useSSL=false", "root", "example");
                System.out.println("Successfully connected");
                break;
            }
            catch (SQLException sqle)
            {
                System.out.println("Failed to connect to database attempt " + Integer.toString(i));
                System.out.println(sqle.getMessage());
            }
            catch (InterruptedException ie)
            {
                System.out.println("Thread interrupted? Should not happen.");
            }
        }
    }

    /**
     * Disconnect from the MySQL database.
     */
    public void disconnect()
    {
        if (con != null)
        {
            try
            {
                // Close connection
                con.close();
            }
            catch (Exception e)
            {
                System.out.println("Error closing connection to database");
            }
        }
    }

    public Employee getEmployee(int ID)
    {
        try
        {
            // Create an SQL statement
            Statement stmt = con.createStatement();
            // Create string for SQL statement
            String strSelect =
                    "SELECT e.emp_no, e.first_name, e.last_name, t.title, s.salary, d.dept_name, " +
                            "m.first_name AS manager " +
                            "FROM employees e " +
                            "JOIN titles t ON t.emp_no = e.emp_no " +
                            "JOIN salaries s ON s.emp_no = e.emp_no AND s.to_date='9999-01-01'" +
                            "JOIN dept_emp de ON de.emp_no = e.emp_no " +
                            "JOIN departments d ON d.dept_no = de.dept_no " +
                            "JOIN dept_manager dm ON dm.dept_no = d.dept_no " +
                            "JOIN employees m ON m.emp_no = dm.emp_no " +
                            "WHERE e.emp_no = " + ID ;



            // Execute SQL statement
            ResultSet rset = stmt.executeQuery(strSelect);
            // Return new employee if valid.
            // Check one is returned
            if (rset.next())
            {
                Employee emp = new Employee();
                emp.emp_no = rset.getInt("emp_no");
                emp.first_name = rset.getString("first_name");
                emp.last_name = rset.getString("last_name");
                emp.title = rset.getString("title");
                emp.salary=rset.getInt("salary");
                emp.dept_name = rset.getString("dept_name");
                emp.manager = rset.getString("manager");
                return emp;
            }
            else
                return null;
        }


        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("Failed to get employee details");
            return null;
        }



    }

    public void displayEmployee(Employee emp)
    {
        if (emp != null)
        {
            System.out.println(
                    emp.emp_no + " "
                            + emp.first_name + " "
                            + emp.last_name + "\n"
                            + emp.title + "\n"
                            + "Salary:" + emp.salary + "\n"
                            + emp.dept_name + "\n"
                            + "Manager: " + emp.manager + "\n");
        }
    }


    //Get the first 10 employees
    public void getEmployees(){
        try {
            // Create SQL statement
            String query = "SELECT emp_no, first_name, last_name FROM employees LIMIT 10";
            Statement stmt = con.createStatement();

            // Execute query
            ResultSet rset = stmt.executeQuery(query);

            // Print each employee
            System.out.println("First 10 Employees:");
            while (rset.next()) {
                int empNo = rset.getInt("emp_no");
                String firstName = rset.getString("first_name");
                String lastName = rset.getString("last_name");
                System.out.println(empNo + ": " + firstName + " " + lastName);
            }
        } catch (SQLException e) {
            System.out.println("SQL Error: " + e.getMessage());
        }
    }

    /**
     * Method to display employee information of all senior engineers
     *
     */
    public void employeeTitle(){

        try {

            //Write the sql query
            String query1 = "SELECT e.emp_no, e.first_name, e.last_name, s.salary, t.title " +
                    "FROM employees e " +
                    "JOIN salaries s ON s.emp_no = e.emp_no " +
                    "JOIN titles t ON t.emp_no = e.emp_no " +
                    "WHERE t.title = 'Senior Engineer' " +
                    "LIMIT 20";


            Statement stmt = con.createStatement();

            // Execute query
            ResultSet rset = stmt.executeQuery(query1);

            //print each employee
            while (rset.next()) {

                int emp_no = rset.getInt("emp_no");
                String first_name = rset.getString("first_name");
                String last_name = rset.getString("last_name");
                String title = rset.getString("title");
                int salary = rset.getInt("salary");

                //print the results
                System.out.println( emp_no + " " + first_name + " " + last_name + "\n"
                                + title + "\n"
                                + "Salary:" + salary + "\n" );
            }
        }

            catch (SQLException e){
                System.out.println("SQL Error: " + e.getMessage());
            }
        }


    public static void main(String[] args)
    {
        // Create new Application
        App a = new App();

        // Connect to database
        a.connect();
        // Get Employee
        Employee emp = a.getEmployee(255530);
        // Display results
        a.displayEmployee(emp);
        System.out.println("Listing the first 10 employees");
        a.getEmployees();

        System.out.println("Listing details on senior engineers");
        a.employeeTitle();

        // Disconnect from database
        a.disconnect();
    }
}

