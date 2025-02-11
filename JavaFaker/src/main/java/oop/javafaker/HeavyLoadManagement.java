package oop.javafaker;

import com.github.javafaker.Faker;


import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HeavyLoadManagement {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/heavyloadmanagement";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";
    private static final int RECORD_COUNT = 10_000_000;
    private static final int THREAD_COUNT = 10;

    public static void main(String[] args) {
        try {
            addRecordsToDatabaseConcurrently();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addRecordsToDatabaseConcurrently() {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.execute(() -> {
                try {
                    addRecordsToDatabase();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Threads didn't finish in the expected time.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Finished inserting records.");
    }

    private static void addRecordsToDatabase() throws SQLException {
        Faker faker = new Faker();
        String insertSQL = "INSERT INTO person (name, email, address, age) VALUES (?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            connection.setAutoCommit(false);

            for (int i = 0; i < RECORD_COUNT / THREAD_COUNT; i++) {
                String name = faker.name().fullName();
                String email = faker.internet().emailAddress();
                String address = faker.address().fullAddress();
                int age = faker.number().numberBetween(18, 99);

                preparedStatement.setString(1, name);
                preparedStatement.setString(2, email);
                preparedStatement.setString(3, address);
                preparedStatement.setInt(4, age);

                preparedStatement.addBatch();

                if (i % 1000 == 0) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    System.out.println(Thread.currentThread().getName() + " inserted " + (i + 1) + " records.");
                }
            }

            preparedStatement.executeBatch();
            connection.commit();
        }
    }
}
