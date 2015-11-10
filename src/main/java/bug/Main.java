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

        main.separateConnections(); // looping in separate connections work
        main.singleConnection(); // looping using single connection fails
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(url, login, password);
    }

    public void singleConnection() throws SQLException {
        System.out.println("Testing using single connection...");

        try (Connection connection = openConnection()) {
            for (int i = 0; i < 10; i++) {
                try {
                    selectEmptyArray(connection);
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
                    selectEmptyArray(connection);
                }
            } catch (Exception e) {
                System.out.println("failed after " + i + " iterations");
                e.printStackTrace();
                return;
            }

        }

        System.out.println("ok");
    }

    private void selectEmptyArray(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT '{}'::int[]")) {
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
