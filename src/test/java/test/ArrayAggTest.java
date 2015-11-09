package test;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class ArrayAggTest {

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost/test", "login", "pass");
    }

    @Test
    public void singleConnection() throws SQLException {
        prepareDatabase();

        try (Connection connection = openConnection()) {
            for (int i = 0; i < 10; i++) {
                try {
                    findDepartmentInfos(connection);
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("failed after " + i + " iterations");
                }
            }
        }
    }

    @Test
    public void separateConnections() throws SQLException {
        prepareDatabase();

        for (int i = 0; i < 10; i++) {
            try (Connection connection = openConnection()) {
                findDepartmentInfos(connection);
            }
        }
    }

    private void prepareDatabase() throws SQLException {
        try (Connection connection = openConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS employee");
                statement.execute("DROP TABLE IF EXISTS department");

                statement.execute("CREATE TABLE department (id INT PRIMARY KEY)");
                statement.execute("CREATE TABLE employee (id INT PRIMARY KEY, department_id INT NOT NULL REFERENCES department)");

                statement.execute("INSERT INTO department (id) VALUES (1)");
            }
        }
    }

    private void findDepartmentInfos(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT array_remove(array_agg(e.id), NULL) FROM department d LEFT JOIN employee AS e ON d.id = e.department_id GROUP BY d.id")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    try (ResultSet ars = rs.getArray(1).getResultSet()) {
                        ars.getMetaData().getColumnLabel(1); // this throws NPE
                    }
                }
            }
        }
    }
}
