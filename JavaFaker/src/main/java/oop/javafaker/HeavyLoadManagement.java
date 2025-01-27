import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ImportFromCSV {
    private static final String URL = "jdbc:postgresql://localhost:5432/heavyloadmanagement";
    private static final String USER = "postgres"; // Replace with your DB username
    private static final String PASSWORD = "password"; // Replace with your DB password
    private static final String INPUT_FILE = "people.csv";
    private static final String INSERT_QUERY = "INSERT INTO PersonAgain (id, name, email, address, age) VALUES (?, ?, ?, ?, ?)";

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(4); // Adjust thread pool size as needed

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             CSVReader reader = new CSVReader(new FileReader(INPUT_FILE))) {

            connection.setAutoCommit(false); // Disable auto-commit for batch processing

            // Skip the header row
            String[] nextLine = reader.readNext();

            while ((nextLine = reader.readNext()) != null) {
                String[] record = nextLine;

                executorService.execute(() -> {
                    try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
                        statement.setInt(1, Integer.parseInt(record[0])); // ID
                        statement.setString(2, record[1]); // Name
                        statement.setString(3, record[2]); // Email
                        statement.setString(4, record[3]); // Address
                        statement.setInt(5, Integer.parseInt(record[4])); // Age
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                // Wait for all threads to finish
            }

            connection.commit(); // Commit all changes to the database
            System.out.println("Import completed.");

        } catch (IOException | SQLException | CsvValidationException e) {
            e.printStackTrace();
        }
    }
}