import com.github.javafaker.Faker;


import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HeavyLoadManagement {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/heavyloadmanagement"; // PostgreSQL URL
    private static final String DB_USER = "postgres"; // PostgreSQL username
    private static final String DB_PASSWORD = "password"; // PostgreSQL password
    private static final int TOTAL_RECORDS = 10_000_000; // Total records to insert
    private static final int THREAD_COUNT = 10; // Number of threads
    private static final AtomicInteger totalRecordsInserted = new AtomicInteger(0); // Tracks total inserted records

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis(); // Record the start time
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        int recordsPerThread = TOTAL_RECORDS / THREAD_COUNT;

        // Start progress display thread
        Thread progressThread = new Thread(() -> {
            while (totalRecordsInserted.get() < TOTAL_RECORDS) {
                try {
                    Thread.sleep(3000); // Wait for 3 seconds
                    int inserted = totalRecordsInserted.get();
                    System.out.println("Inserted " + inserted + " records so far out of " + TOTAL_RECORDS + " records.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        progressThread.start();

        // Launch threads for data insertion
        for (int i = 0; i < THREAD_COUNT; i++) {
            int start = i * recordsPerThread + 1;
            int end = (i == THREAD_COUNT - 1) ? TOTAL_RECORDS : start + recordsPerThread - 1;
            executorService.execute(new DataInserter(start, end));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all threads to finish
        }

        long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime) / 1000; // Time in seconds
        long minutes = elapsedTime / 60;
        long seconds = elapsedTime % 60;
        System.out.println("The data insertion process has completed successfully.");
        System.out.println("Total time for insertion: " + minutes + " minutes and " + seconds + " seconds.");
    }

    static class DataInserter implements Runnable {
        private final int start;
        private final int end;

        DataInserter(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            Faker faker = new Faker();
            String insertSQL = "INSERT INTO person (name, email, address, age) VALUES (?, ?, ?, ?)"; // Keeping the 'person' table

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                connection.setAutoCommit(false); // Disable auto-commit for batch processing

                for (int i = start; i <= end; i++) {
                    preparedStatement.setString(1, faker.name().fullName().replace(",", "")); // Remove commas
                    preparedStatement.setString(2, faker.internet().emailAddress().replace(",", ""));
                    preparedStatement.setString(3, faker.address().streetAddress().replace(",", ""));
                    preparedStatement.setInt(4, faker.number().numberBetween(18, 99)); // Random age between 18 and 99
                    preparedStatement.addBatch();

                    if (i % 10_000 == 0) {
                        preparedStatement.executeBatch(); // Execute batch every 10,000 records
                        connection.commit(); // Commit the transaction after each batch
                    }

                    // Update the total inserted records count
                    totalRecordsInserted.incrementAndGet();
                }

                preparedStatement.executeBatch(); // Execute remaining records
                connection.commit();  // Commit the final batch
                System.out.println("Thread " + Thread.currentThread().getName() + " successfully inserted records from " + start + " to " + end);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
