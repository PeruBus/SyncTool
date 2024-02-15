package org.perubus;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.Files.deleteIfExists;
import static org.perubus.JdbcSingleton.getConnection;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        String fileURL = "https://www2.sunat.gob.pe/padron_reducido_ruc.zip";
        String fileName = "padron_reducido_ruc.zip";

//        Path filePath = Paths.get(fileName);
//        Path finalPath = Paths.get("decompressed/padron_reducido_ruc.txt");
//        deleteIfExists(filePath);
//        deleteIfExists(finalPath);
//
//        // Download
//        downloadFile(fileName, fileURL);
//        System.out.println("\rUnzipping...");
//
//        //Extract
//        extractFile(fileName);
//        System.out.flush();
//        System.out.println("Complete extraction.");

        // Read and put in Database
        try {
            insertTWPDRN("decompressed/padron_reducido_ruc.txt");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    static void insertTWPDRN(String finalPath) {
        Path path = Paths.get(finalPath);

        try (AsynchronousFileChannel asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)) {
            int fileSize = (int) asyncChannel.size();
            ByteBuffer buffer = ByteBuffer.allocate(fileSize);
            CountDownLatch latch = new CountDownLatch(1);

            asyncChannel.read(buffer, 0, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    System.out.println("Number of bytes read: " + result);

                    attachment.flip();
                    byte[] dataBytes = new byte[result];
                    attachment.get(dataBytes);

                    // Convert the byte array to a string using StandardCharsets.UTF_8
                    readAndPrintLines(dataBytes);

                    attachment.clear();
                    latch.countDown();
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    System.out.println("Read operation failed - " + exc.getMessage());
                    latch.countDown();
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            System.out.println("Something unexpected happened.");
        }
    }
    static void readAndPrintLines(byte[] dataBytes) {
        List<Data> dataSet = new ArrayList<>();
        int start = 0;
        for (int i = 1; i < dataBytes.length; i++) {
            if (dataBytes[i] == '\n') {
                String line = new String(dataBytes, start, i - start, StandardCharsets.UTF_8);
                String[] data = line.split("\\|");
                System.out.println(i + " Line read: " + data[0] + data[1]);
                dataSet.add(new Data(data[0],data[1]));
                /////
                JdbcSingleton.insertData(data[0], data[1]);
                start = i + 1;
            }
        }

        if (start < dataBytes.length) {
            String lastLine = new String(dataBytes, start, dataBytes.length - start, StandardCharsets.UTF_8);
            String[] data = lastLine.split("\\|");
            dataSet.add(new Data(data[0],data[1]));
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

    public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    static private void downloadFile(String fileName, String fileURL) throws IOException {
        URL website = new URL(fileURL);
        URLConnection connection = website.openConnection();
        int fileSize = connection.getContentLength();

        ReadableByteChannel rbc = Channels.newChannel(connection.getInputStream());

        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            long transferredBytes = 0;
            long startTime = System.currentTimeMillis();
            int bufferSize = 4481024;

            System.out.println("Downloading...");

            while (true) {
                long bytesTransferred = fos.getChannel().transferFrom(rbc, transferredBytes, bufferSize);

                if (bytesTransferred == 0) {
                    break;
                }
                transferredBytes += bytesTransferred;
                showProgress(transferredBytes, fileSize);

                Thread.sleep(50);
            }

            long endTime = System.currentTimeMillis();
            System.out.println("\nDownload completed in " + (endTime - startTime) / 1000 + " seconds");


        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
        } finally {
            rbc.close();
        }

    }

    private static void showProgress(long transferred, long total) {
        int progress = (int) ((transferred * 100) / total);
        System.out.print("\rProgress: " + progress + "%");
        System.out.flush();

        if (transferred == total) {
            System.out.println();
        }
    }
}
