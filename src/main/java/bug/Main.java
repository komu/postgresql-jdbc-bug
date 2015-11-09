package bug;

import java.sql.*;

public class Main {

    private final String url;
    private final String login;
    private final String password;

    public Main(String url, String login, String password) {
        this.url = url;
        this.login = login;
        this.password = password;
    }

    public static void main(String[] args) throws SQLException {
        if (args.length != 3) {
            System.err.println("usage: bug.Main url login password");
            System.exit(1);
        }

        Main main = new Main(args[0], args[1], args[2]);

        main.prepareDatabase();
        main.separateConnections();
        main.singleConnection();
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(url, login, password);
    }

    public void singleConnection() throws SQLException {
        System.out.println("Testing using single connection...");

        try (Connection connection = openConnection()) {
            for (int i = 0; i < 10; i++) {
                try {
                    findDepartmentInfos(connection);
                } catch (Exception e) {
                    System.out.println("failed after " + i + " iterations");
                    e.printStackTrace();
                    return;
                }
            }
        }

        System.out.println("ok");
    }

    public void separateConnections() throws SQLException {
        System.out.println("Testing using separate connections...");

        for (int i = 0; i < 10; i++) {
            try {
                try (Connection connection = openConnection()) {
                    findDepartmentInfos(connection);
                }
            } catch (Exception e) {
                System.out.println("failed after " + i + " iterations");
                e.printStackTrace();
                return;
            }

        }

        System.out.println("ok");
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
                        ars.getMetaData().getColumnType(1); // this throws NPE
                    }
                }
            }
        }
    }
}
