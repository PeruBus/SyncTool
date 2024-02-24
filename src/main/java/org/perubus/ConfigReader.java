package org.perubus;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    private String fileUrl;
    private String fileName;
    private String finalPathString;
    private int defaultBatchSize;
    private String linkUrl;
    private String databaseUser;
    private String passwordUser;
    private boolean trustServerCertificate;
    private boolean rewriteBatchedStatements;
    private int threadBulk;
    public ConfigReader(String filePath){
        loadProperties(filePath);
    }
    private void loadProperties(String filePath) {
        Properties properties = new Properties();
        try(FileInputStream fileInputStream = new FileInputStream(filePath)){
            properties.load(fileInputStream);

            fileUrl = properties.getProperty("fileURL");
            fileName = properties.getProperty("fileName");
            finalPathString = properties.getProperty("finalPathString");
            defaultBatchSize = Integer.parseInt(properties.getProperty("DEFAULT_BATCH_SIZE"));
            linkUrl = properties.getProperty("LINK_URL");
            databaseUser = properties.getProperty("databaseUser");
            passwordUser = properties.getProperty("passwordUser");
            trustServerCertificate = Boolean.parseBoolean(properties.getProperty("trustServerCertificate"));
            rewriteBatchedStatements = Boolean.parseBoolean(properties.getProperty("rewriteBatchedStatements"));
            threadBulk = Integer.parseInt(properties.getProperty("threadBulk"));

        }catch (IOException exception){
            exception.printStackTrace();
        }
    }
    public String getFileUrl() {
        return fileUrl;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFinalPathString() {
        return finalPathString;
    }

    public int getDefaultBatchSize() {
        return defaultBatchSize;
    }

    public String getLinkUrl() {
        return linkUrl;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public String getPasswordUser() {
        return passwordUser;
    }

    public boolean isTrustServerCertificate() {
        return trustServerCertificate;
    }

    public boolean isRewriteBatchedStatements() {
        return rewriteBatchedStatements;
    }

    public int getThreadBulk() {
        return threadBulk;
    }

}
