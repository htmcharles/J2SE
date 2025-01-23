package oop.javafaker;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ImportFromCSV {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/heavyloadmanagement";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final String CSV_FILE = "people.csv";
    private static final int THREAD_COUNT = 5;  // Reduced number of threads
    private static final int BATCH_SIZE = 1000;  // Size of each batch

    public static void main(String[] args) {
        try {
            importRecordsFromCSVConcurrently();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void importRecordsFromCSVConcurrently() {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<String[]> batchRecords = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE))) {
            String[] nextLine;

            // Skip the header line
            reader.readNext();

            while ((nextLine = reader.readNext()) != null) {
                batchRecords.add(nextLine);

                // If batch size reached, submit a thread to insert the batch
                if (batchRecords.size() >= BATCH_SIZE) {
                    final List<String[]> batch = new ArrayList<>(batchRecords);
                    executor.execute(() -> insertBatchToDatabase(batch));
                    batchRecords.clear();  // Clear the batch for the next set of records
                }
            }

            // Insert any remaining records after the loop
            if (!batchRecords.isEmpty()) {
                executor.execute(() -> insertBatchToDatabase(batchRecords));
            }

            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Threads didn't finish in the expected time.");
            }
            System.out.println("Finished importing records from CSV.");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertBatchToDatabase(List<String[]> batchRecords) {
        String insertSQL = "INSERT INTO backup (id, name, email, address, age) VALUES (?, ?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            connection.setAutoCommit(false);

            // Process each record in the batch
            for (String[] record : batchRecords) {
                preparedStatement.setInt(1, Integer.parseInt(record[0]));  // ID
                preparedStatement.setString(2, record[1]);                // Name
                preparedStatement.setString(3, record[2]);                // Email
                preparedStatement.setString(4, record[3]);                // Address
                preparedStatement.setInt(5, Integer.parseInt(record[4])); // Age

                preparedStatement.addBatch();
            }

            // Execute the batch insert
            preparedStatement.executeBatch();
            connection.commit();
            System.out.println(Thread.currentThread().getName() + " batch inserted " + batchRecords.size() + " records.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
