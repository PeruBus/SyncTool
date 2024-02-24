package org.perubus;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.Files.deleteIfExists;
import static org.perubus.JdbcSingleton.*;

/**
 * Sync Application for Download some data, something simple really!
 */
public class App {
    static private final List<Data> dataSet = new ArrayList<>();

    public static void main(String[] args) {

//        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//
//        // Get the current time
//        LocalTime currentTime = LocalTime.now();
//
//        System.out.println("TODAY: " +currentTime);
//        // Calculate the initial delay until 1 a.m.
//        long initialDelay = calculateInitialDelay(currentTime);
//
//        // First Execution
        try {
            truncateData();
            completeLifeCycle();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
//        // Schedule the task to run every 24 hours
//        scheduler.scheduleAtFixedRate(App::scheduleTask, initialDelay, 24, TimeUnit.HOURS);
    }
    private static void scheduleTask(){
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Get the current time
        LocalTime currentTime = LocalTime.now();

        System.out.println("EXECUTION BEING MADE at: " + currentTime);
        // Calculate the initial delay until 1 a.m.
        long initialDelay = calculateInitialDelay(currentTime);

        // Schedule the task to run every 24 hours
        scheduler.scheduleAtFixedRate(() -> {
            try {
                completeLifeCycle();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }, initialDelay, 24, TimeUnit.HOURS);
    }
    private static long calculateInitialDelay(LocalTime currentTime) {
        // Calculate the initial delay until 1 a.m.
        LocalTime targetTime = LocalTime.of(1, 0);
        long initialDelay = LocalTime.from(currentTime).until(targetTime, TimeUnit.HOURS.toChronoUnit());
        if (initialDelay < 0) {
            // If the current time is after 1 a.m., add 24 hours to schedule it for the next day
            initialDelay += 24;
        }
        return initialDelay;
    }

    static void completeLifeCycle() throws IOException, URISyntaxException {
        truncateData();
        ConfigReader configReader = new ConfigReader("application.properties");
        String fileURL = configReader.getFileUrl();
        String fileName = configReader.getFileName();
        String finalPathString = configReader.getFinalPathString();

        // Paths
        Path filePath = Paths.get(fileName);
        Path finalPath = Paths.get(finalPathString);
        deleteIfExists(filePath);
        deleteIfExists(finalPath);
//
        // Download
        downloadFile(fileName, fileURL);
        System.out.println("\rUnzipping...");
//
         //Extract
        extractFile(fileName);
        System.out.flush();
        System.out.println("Complete extraction.");

        // Read and put in Database
        alternativeTWPDRN(finalPathString);

        // List the data
        try {
            long freeMemory = Runtime.getRuntime().freeMemory();
            long totalMemory = Runtime.getRuntime().totalMemory();
            System.out.println("\nAmount of MEMORY FREE: " + freeMemory + " , total: " + totalMemory);
            sendBulkAsync(dataSet);
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }

    static void alternativeTWPDRN(String filePath) {
        System.out.println("\rReading file...");
        System.out.flush();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split("\\|");
                dataSet.add(new Data(data[0], data[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    static private void downloadFile(String fileName, String fileURL) throws URISyntaxException {
        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(fileURL))
                    .build();

            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            int fileSize = response.headers().firstValue("Content-Length")
                    .map(Integer::parseInt)
                    .orElse(-1);

            try (InputStream inputStream = response.body();
                 FileOutputStream outputStream = new FileOutputStream(fileName)) {

                byte[] buffer = new byte[8192]; // Adjust buffer size as needed
                int bytesRead;
                int totalBytesRead = 0;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                    // Add progress or other handling if needed
                    showProgress(totalBytesRead, fileSize);                }

                if (fileSize != -1 && totalBytesRead != fileSize) {
                    System.out.println("Warning: File size mismatch!");
                }
                System.out.println("Download complete.");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void showProgress(long total, long fileSize) {
        long totalInMB = total / (1024 * 1024); // Convert bytes to megabytes

        int progress = (int) ((total * 100) / fileSize);
        System.out.print("\rProgress: " + progress + "% (" + fileSize / (1024 * 1024) + " MB / " + totalInMB + " MB downloaded)");
        System.out.flush();

        if (total == fileSize) {
            System.out.println();
        }
    }

    static private void extractFile(String fileName) throws IOException {
        File destdir = new File("decompressed/");
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(fileName));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(destdir, zipEntry);
            if (zipEntry.isDirectory()) {
                if (!newFile.isDirectory() && !newFile.mkdirs()) {
                    throw new IOException("Failed to create directory " + newFile);
                }
            } else {
                // fix for Windows-created archives
                File parent = newFile.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("Failed to create directory " + parent);
                }

                // write file content
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
    }


    ////////////////////////////NEW FILE///////////////////
//    static void insertTWPDRN(String finalPath) {
//        Path path = Paths.get(finalPath);
//
//        try (AsynchronousFileChannel asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)) {
//            int fileSize = (int) asyncChannel.size();
//            ByteBuffer buffer = ByteBuffer.allocate(fileSize);
//            CountDownLatch latch = new CountDownLatch(1);
//
//            asyncChannel.read(buffer, 0, buffer, new CompletionHandler<Integer, ByteBuffer>() {
//                @Override
//                public void completed(Integer result, ByteBuffer attachment) {
//                    System.out.println("Number of bytes read: " + result);
//
//                    attachment.flip();
//                    byte[] dataBytes = new byte[result];
//                    attachment.get(dataBytes);
//
//                    // Convert the byte array to a string using StandardCharsets.UTF_8
//                    readAndPrintLines(dataBytes);
//
//                    attachment.clear();
//                    latch.countDown();
//                }
//
//                @Override
//                public void failed(Throwable exc, ByteBuffer attachment) {
//                    System.out.println("Read operation failed - " + exc.getMessage());
//                    latch.countDown();
//                }
//            });
//
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        } catch (Exception exception) {
//            exception.printStackTrace();
//            System.out.println("Something unexpected happened.");
//        }
//    }

//    static void readAndPrintLines(byte[] dataBytes) {
//        int start = 0;
//        for (int i = 1; i < dataBytes.length; i++) {
//            if (dataBytes[i] == '\n') {
//                String line = new String(dataBytes, start, i - start, StandardCharsets.UTF_8);
//                String[] data = line.split("\\|");
////                System.out.println(i + " Line read: " + data[0] + data[1]);
//                dataSet.add(new Data(data[0], data[1]));
//                /////
////                JdbcSingleton.insertData(data[0], data[1]);
//                start = i + 1;
//            }
//        }
//
//        if (start < dataBytes.length) {
//            String lastLine = new String(dataBytes, start, dataBytes.length - start, StandardCharsets.UTF_8);
//            String[] data = lastLine.split("\\|");
//            dataSet.add(new Data(data[0], data[1]));
//        }
//    }

}
