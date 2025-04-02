package org.yinuo.rabbitmq101;

import com.rabbitmq.client.*;
import com.rabbitmq.client.Connection;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MQConsumer {
  private static final String QUEUE_NAME = "likeQueue";

  private static final int NUM_THREADS = 5;

  /**
   * Rewrites the main function to enable this consumer called from the server.
   */
  public static void startConsuming() throws Exception {
    // Initialize RabbitMQ connection once and reuse the same connections
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final com.rabbitmq.client.Connection connection = factory.newConnection();

    // Start multiple consumer threads
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread thread = new Thread(() -> {
        try {
          // Create a database connection per thread
          Class.forName("com.mysql.cj.jdbc.Driver");
          java.sql.Connection dbConnection = DriverManager.getConnection(
              "jdbc:mysql://database-1.ckttmr66bufd.us-west-2.rds.amazonaws.com:3306/album_store?useSSL=false&allowPublicKeyRetrieval=true",
              "admin",
              "20011016"
          );

          PreparedStatement insertStatement = dbConnection.prepareStatement(
              "INSERT INTO album_reviews (album_id, review_type) VALUES (?, ?)"
                  + " ON DUPLICATE KEY UPDATE review_type = VALUES(review_type)");

          Channel channel = connection.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          channel.basicQos(1); // Only one unacked message per consumer

          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Thread " + Thread.currentThread().getId() + " received: " + message);

            String[] parts = message.split(",");
            int albumId = Integer.parseInt(parts[0]);
            String reviewType = parts[1];

            try {
              insertStatement.setInt(1, albumId);
              insertStatement.setString(2, reviewType);
              insertStatement.executeUpdate();
              System.out.println("✅ Review saved: album_id=" + albumId + ", review_type=" + reviewType);
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (SQLException e) {
              e.printStackTrace();
            }
          };

          channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      thread.start();
    }
  }
//  public static void startConsuming() throws Exception {
//    // Initialize MySQL connection
//    Class.forName("com.mysql.cj.jdbc.Driver");
//    java.sql.Connection dbConnection = DriverManager.getConnection(
//        "jdbc:mysql://database-1.ckttmr66bufd.us-west-2.rds.amazonaws.com:3306/album_store?useSSL=false&allowPublicKeyRetrieval=true",
//        "admin",
//        "20011016"
//    );
//
//    insertStatement = dbConnection.prepareStatement(
//        "INSERT INTO album_reviews (album_id, review_type) VALUES (?, ?)"
//    + " ON DUPLICATE KEY UPDATE review_type = VALUES(review_type)");
//
//    ConnectionFactory factory = new ConnectionFactory();
//    factory.setHost("localhost");
//    Connection connection = factory.newConnection();
//    Channel channel = connection.createChannel();
//
//    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
//    System.out.println("✅ Waiting for review messages...");
//
//    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//      System.out.println("📥 Received Review: " + message);
//
//      // Parse message format: "albumId,reviewType"
//      String[] parts = message.split(",");
//      int albumId = Integer.parseInt(parts[0]);
//      String reviewType = parts[1];
//
//      // Insert into MySQL
//      saveReviewToDatabase(albumId, reviewType);
//
//      // Acknowledge message processing
//      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//    };
//
//    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
//
//  }

//  private static void saveReviewToDatabase(int albumId, String reviewType) {
//    try {
//      insertStatement.setInt(1, albumId);
//      insertStatement.setString(2, reviewType);
//      insertStatement.executeUpdate();
//      System.out.println("✅ Review saved to database: album_id=" + albumId + ", review_type=" + reviewType);
//    } catch (SQLException e) {
//      e.printStackTrace();
//    }
//  }
}
