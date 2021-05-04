package requests;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.util.ArrayList;


import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.google.gson.Gson;
import org.apache.commons.dbutils.QueryRunner;


public class Main {

    public static void main(String[] args) throws ManagedProcessException, SQLException {
//        MariaDB Embedded Database setup
        DBConfigurationBuilder config = DBConfigurationBuilder.newBuilder();
        config.setPort(3306);
        DB db = DB.newEmbeddedDB(config.build());
        db.start();
        String dbName = "testdb";
        db.createDB(dbName);

//        Connect to embedded DB, then setup table
        Connection connection;
        String jdbc = "jdbc:mysql://localhost/" + dbName + "?serverTimezone=UTC";
        try {
            connection = DriverManager.getConnection(jdbc, "root", "");
            QueryRunner qr = new QueryRunner();
            qr.update(connection, "CREATE TABLE messages (" +
                    "msg_id BIGINT PRIMARY KEY, " +
                    "company_name VARCHAR(255), " +
                    "registration_date DATETIME NOT NULL, " +
                    "score FLOAT, " +
                    "directors_count INT, " +
                    "last_updated DATETIME);");
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        Rest request setup
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("https://run.mocky.io/v3/aff84616-f430-4842-9b76-eb2cf4a5eaeb")) //Mock API server
                .build();

//        Make rest request, then parse JSON, then write to DB
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenApply(Main::parseMessages)
                .thenApply(Parser::getMessages)
                .thenAccept(Main::writeToDB)
                .join();

//        Testing/Debug - See if INSERT rows worked vs. JSON received
        connection = DriverManager.getConnection(jdbc, "root", "");
        PreparedStatement statement;
        String query = "SELECT * FROM messages";
        statement = connection.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println( );
            System.out.println("Database Row Details: ");
            System.out.println("Id: " + rs.getString("msg_id"));
            System.out.println("Company_Name: " + rs.getString("company_name"));
            System.out.println("reg_date: " + rs.getString("registration_date"));
            System.out.println("score: " + rs.getString("score"));
            System.out.println("dir_counts: " + rs.getString("directors_count"));
            System.out.println("last_updated: " + rs.getString("last_updated") + "\n");
        }
        statement.close(); // Close All Connections
        rs.close();
        connection.close();
        db.stop();
    }

    private static Parser parseMessages(String data) {
        System.out.println(data); //Raw JSON data for comparison with testing above
        Gson gson = new Gson();
        return gson.fromJson(data, Parser.class); //Parse JSON into instances of Message
    }

    private static void writeToDB(ArrayList<Message> messages) {
        for (Message message: messages) {
            writeToDB(message);
        }
    }

    private static void writeToDB(Message message) {
        try {
            Connection connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/testdb?serverTimezone=UTC", "root", "");
            String regDate = processDate(message.getReg_date());
            String lastUpdate = processDate(message.getLast_updated());
            String companyName = "'" + message.getCompany_name() + "'";
            String sql = "INSERT INTO messages (msg_id, company_name, registration_date, score, directors_count, " +
                    "last_updated) " +
                    "VALUES (" + message.getMsg_id() + ", " + companyName + ", " + regDate + ", " +
                    message.getScore() + ", " + message.getDir_count() + ", " + lastUpdate + ") " +
                    "ON DUPLICATE KEY UPDATE company_name=" + companyName +
                    ", registration_date=" + regDate +
                    ", score=" + message.getScore() +
                    ", directors_count=" + message.getDir_count() +
                    ", last_updated=" + lastUpdate;
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.execute();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String processDate(String date) {
        StringBuilder processed = new StringBuilder("'");
        for (int i = 0; i < 10; i++) {
            processed.append(date.charAt(i));
        }
        processed.append(" ");
        for (int i = 12; i < 20; i++) {
            processed.append(date.charAt(i));
        }
        processed.append("'");
        return processed.toString();
    }
}