package dbms.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class dbConnectionTest {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/DBsystem";
        String username = "root";
        String password = "2264";

        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Database connected successfully.");
            connection.close();
        } catch (SQLException e) {
            System.out.println("Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}