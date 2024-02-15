package org.perubus;

import java.sql.*;

public class JdbcSingleton {
    private static Connection connection = null;

    static {
        String url = "jdbc:sqlserver://10.10.10.60:1433;databaseName=SISDOCU;trustServerCertificate=true";
        String user = "dba01";
        String pass = "$dbjma/";
        try {
            connection = DriverManager.getConnection(url, user, pass);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if(connection != null){
            try {
                DatabaseMetaData dm = (DatabaseMetaData)  connection.getMetaData();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }
    public static Connection getConnection(){
        if(connection != null){
            return connection;
        }
        else return null;
    }
    // Example method to insert data into a table
    public static void insertData(String coClie, String noClie) {
        Connection connection = getConnection();
        if (connection != null) {
            String insertQuery = "INSERT INTO TWPDRN (CO_CLIE, NO_CLIE) VALUES (?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                preparedStatement.setString(1, coClie);
                preparedStatement.setString(2, noClie);

                int rowsAffected = preparedStatement.executeUpdate();

                System.out.println(rowsAffected + " row(s) inserted successfully.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Connection is null. Unable to insert data.");
        }
    }
}
