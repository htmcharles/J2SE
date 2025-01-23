package oop.javaFaker;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class ExportToCSV {
    private static final String URL = "jdbc:mysql://localhost:3306/heavyloadmanagement";
    private static final String USER = "root"; // Replace with your DB username
    private static final String PASSWORD = ""; // Replace with your DB password
    private static final String OUTPUT_FILE = "people.csv";

    public static void main(String[] args) {
        String selectQuery = "SELECT * FROM Person";

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement(selectQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             CSVWriter writer = new CSVWriter(new FileWriter(OUTPUT_FILE))) {

            // Enable MySQL streaming mode
            statement.setFetchSize(Integer.MIN_VALUE);

            try (ResultSet resultSet = statement.executeQuery()) {
                // Write CSV headers
                String[] headers = {"ID", "Name", "Email", "Address", "Age"};
                writer.writeNext(headers);

                int recordCount = 0;
                while (resultSet.next()) {
                    String[] record = {
                            String.valueOf(resultSet.getInt("id")),
                            resultSet.getString("name"),
                            resultSet.getString("email"),
                            resultSet.getString("address"),
                            String.valueOf(resultSet.getInt("age"))
                    };
                    writer.writeNext(record);

                    recordCount++;
                    if (recordCount % 10_000 == 0) {
                        System.out.println("Exported " + recordCount + " records...");
                    }
                }
                System.out.println("Export completed. Total records exported: " + recordCount);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
