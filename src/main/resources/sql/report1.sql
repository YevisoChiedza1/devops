SELECT e.emp_no, e.first_name, e.last_name, t.title, s.salary, d.dept_name,
                            m.first_name AS manager
                            FROM employees e
                            JOIN titles t ON t.emp_no = e.emp_no
                            JOIN salaries s ON s.emp_no = e.emp_no AND s.to_date='9999-01-01'
                            JOIN dept_emp de ON de.emp_no = e.emp_no
                            JOIN departments d ON d.dept_no = de.dept_no
                            JOIN dept_manager dm ON dm.dept_no = d.dept_no
                            JOIN employees m ON m.emp_no = dm.emp_no
                            WHERE e.emp_no =  + ID ;