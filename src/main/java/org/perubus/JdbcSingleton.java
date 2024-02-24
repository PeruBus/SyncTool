package org.perubus;

import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JdbcSingleton {
    private static Connection connection;
    private static ConfigReader configReader = new ConfigReader("application.properties");
    private static final int DEFAULT_BATCH_SIZE = configReader.getDefaultBatchSize();
    static {
        String url = configReader.getLinkUrl();
        String user  = configReader.getDatabaseUser();
        String pass = configReader.getPasswordUser();
        Properties props = new Properties();
        props.put("trustServerCertificate", String.valueOf(configReader.isTrustServerCertificate()));
        props.put("user", user);
        props.put("password", pass);
        props.put("rewriteBatchedStatements",String.valueOf(configReader.isRewriteBatchedStatements()));
        try {
            connection = DriverManager.getConnection(url, props);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if(connection != null){
            try {
                DatabaseMetaData dm = connection.getMetaData();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static void sendBulkAsync(List<Data> dataList) {
        ExecutorService executorService = Executors.newFixedThreadPool(configReader.getThreadBulk());

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                sendBulk(dataList, connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, executorService);

        // Wait for the completion of the asynchronous task
        future.join();

        // Shutdown the executor service
        executorService.shutdown();
        try {
            // Wait for the executor service to terminate gracefully
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void truncateData(){
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("TRUNCATE TABLE " + "TWPDRN");
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sendBulk(List<Data> dataList, Connection connection) throws SQLException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("insert into TWPDRN(CO_CLIE, NO_CLIE) values (?,?)");

            long start = System.currentTimeMillis();
            int batchSize = calculateBatchSize();

            for (int i = 0; i < dataList.size(); i++) {
                preparedStatement.setString(1, dataList.get(i).CO_CLIE());
                preparedStatement.setString(2, dataList.get(i).NO_CLIE());
                preparedStatement.addBatch();
                showStatus(i, dataList.size(), start);

                if (i % batchSize == 0) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
            }

            preparedStatement.executeBatch();
            connection.commit();

            long end = System.currentTimeMillis();

            System.out.println("Finished. Time taken: " + (end - start) + " milliseconds.");
        } catch (OutOfMemoryError exception) {
            System.out.println("\r" + exception.getMessage());
            System.out.flush();
        }
    }

    private static int calculateBatchSize() {
        // Calculate the available free memory
        long freeMemory = Runtime.getRuntime().freeMemory();

        // Adjust the batch size based on available memory
        if (freeMemory < 100_000_000) {  // Adjust this threshold based on your requirements
            return DEFAULT_BATCH_SIZE / 2; // Reduce batch size if available memory is low
        } else {
            return DEFAULT_BATCH_SIZE;
        }
    }
    private static void showStatus(int processed, int total, long startTime) {
        int progress = (processed * 100) / total;
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.print("\rData processed: " + processed + "/" + total + " (" + progress + "%) Elapsed time: " + formatElapsedTime(elapsedTime));
        System.out.flush();
    }
    private static String formatElapsedTime(long elapsedTime) {
        long seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) % 60;
        long minutes = TimeUnit.MILLISECONDS.toMinutes(elapsedTime) % 60;
        long hours = TimeUnit.MILLISECONDS.toHours(elapsedTime);

        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    /////////////////////ONLY FOR TESTING////////////////
//    public static Connection getConnection(){
//        if(connection != null){
//            return connection;
//        }
//        else return null;
//    }
//    public static void insertData(String coClie, String noClie) {
//        Connection connection = getConnection();
//        if (connection != null) {
//            String insertQuery = "INSERT INTO TWPDRN (CO_CLIE, NO_CLIE) VALUES (?, ?)";
//
//            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
//                preparedStatement.setString(1, coClie);
//                preparedStatement.setString(2, noClie);
//
//                int rowsAffected = preparedStatement.executeUpdate();
//
//                System.out.println(rowsAffected + " row(s) inserted successfully.");
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("Connection is null. Unable to insert data.");
//        }
//    }

//    public static void sendBulk() throws SQLException {
//        connection = getConnection();
//        Statement stmt = connection.createStatement();
//        PreparedStatement preparedStatement = connection.prepareStatement("insert into TWPDRN(CO_CLIE, NO_CLIE) values (?,?)");
//
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < 16_000_000; i++) {
//            preparedStatement.setString(1, "FIRST");
//            preparedStatement.setString(2, "SECOND");
//            preparedStatement.addBatch();
//            if (i % 1000 == 0) {
//                preparedStatement.executeBatch();
//                connection.commit();
//            }
//        }
//        preparedStatement.executeBatch();
//        connection.commit();
//
//        long end = System.currentTimeMillis();
//
//        System.out.println("Finished. Time taken : " + (end - start) + " milliseconds.");
//    }

}
