package org.yinuo.rabbitmq101;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

@WebServlet(name = "Server", value = "/*")
@MultipartConfig
public class Server extends HttpServlet {
  private java.sql.Connection writeConnection;
  private java.sql.Connection readConnection;
  private static final String QUEUE_NAME = "likeQueue";
  private com.rabbitmq.client.Connection mqConnection;

  // Channel pool for channel reuse
  private BlockingQueue<Channel> channelPool;
  private static final int CHANNEL_POOL_SIZE = 10;

  @Override
  public void init() throws ServletException {
    try {
      // Initialize JDBC MySQL connection
      Class.forName("com.mysql.cj.jdbc.Driver");

      java.sql.Connection initialConnection = DriverManager.getConnection(
          "jdbc:mysql://database-1.ckttmr66bufd.us-west-2.rds.amazonaws.com:3306/?useSSL=false&allowPublicKeyRetrieval=true",
          "admin",
          "20011016"
      );
      // Create the database if it doesn't exist
      try (Statement stmt = initialConnection.createStatement()) {
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS album_store;");
      }
      initialConnection.close();

      writeConnection = DriverManager.getConnection(
          "jdbc:mysql://database-1.ckttmr66bufd.us-west-2.rds.amazonaws.com:3306/album_store?useSSL=false&allowPublicKeyRetrieval=true",
          "admin",
          "20011016"
      );

      readConnection = DriverManager.getConnection(
          "jdbc:mysql://replica.ckttmr66bufd.us-west-2.rds.amazonaws.com:3306/album_store?useSSL=false&allowPublicKeyRetrieval=true",
          "admin",
          "20011016"
      );

      // Initialize RabbitMQ connection
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      mqConnection = factory.newConnection();
      channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);

      for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
        Channel channel = mqConnection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channelPool.add(channel);
      }

      createTables();
      System.out.println("Tables created successfully.");
    } catch (Exception e) {
      throw new ServletException("Database connection failed", e);
    }
  }

  private void createTables() throws SQLException {
    String createAlbumsTable = "CREATE TABLE IF NOT EXISTS albums ("
        + "id INT AUTO_INCREMENT PRIMARY KEY,"
        + "artist VARCHAR(255) NOT NULL,"
        + "title VARCHAR(255) NOT NULL,"
        + "year INT NOT NULL,"
        + "image LONGBLOB,"
        + "image_size BIGINT"
        + ")";

    String createReviewsTable = "CREATE TABLE IF NOT EXISTS album_reviews ("
        + "id INT AUTO_INCREMENT PRIMARY KEY,"
        + "album_id INT NOT NULL,"
        + "review_type ENUM('like', 'dislike'),"
        + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        + "FOREIGN KEY (album_id) REFERENCES albums(id)"
        + ")";

    try (Statement stmt = writeConnection.createStatement()) {
      stmt.execute(createAlbumsTable);
      stmt.execute(createReviewsTable);
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    String url = request.getPathInfo();

    response.getWriter().write("DEBUG: Received URL path: " + url + "\n");

    if (url == null || url.equals("/albums")) {
      // Case 1: Get all albums
      getAllAlbums(response);
    } else {
      String[] urlParts = url.split("/");

      response.getWriter().write("DEBUG: Split URL parts: " + Arrays.toString(urlParts) + "\n");

      if (urlParts.length == 3) {
        // Case 2: Get specific album by ID
        getAlbumById(urlParts[2], response);
      } else if (urlParts.length == 4 && urlParts[3].equals("reviews")) {
        // Case 3: Get album reviews (like/dislike count)
        getAlbumReviews(urlParts[2], response);
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        response.getWriter().write("{\"error\": \"Invalid request\"}");
      }
    }
  }

  private void getAllAlbums(HttpServletResponse response) throws IOException {
    try (Statement stmt = readConnection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT id, artist, title, year FROM albums")) {

      List<Album> albums = new ArrayList<>();
      while (rs.next()) {
        albums.add(new Album(
            rs.getString("artist"),
            rs.getString("title"),
            rs.getInt("year"),
            rs.getInt("id")
        ));
      }
      response.getWriter().write(new Gson().toJson(albums));

    } catch (SQLException e) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("{\"error\": \"Database error: " + e.getMessage() + "\"}");
    }
  }

  private void getAlbumById(String albumId, HttpServletResponse response) throws IOException {
    try (PreparedStatement stmt = readConnection.prepareStatement(
        "SELECT id, artist, title, year FROM albums WHERE id = ?")) {
      stmt.setInt(1, Integer.parseInt(albumId));
      ResultSet rs = stmt.executeQuery();

      if (rs.next()) {
        Album album = new Album(
            rs.getString("artist"),
            rs.getString("title"),
            rs.getInt("year"),
            rs.getInt("id")
        );
        response.getWriter().write(new Gson().toJson(album));
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        response.getWriter().write("{\"error\": \"Album not found\"}");
      }

    } catch (SQLException e) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("{\"error\": \"Database error: " + e.getMessage() + "\"}");
    }
  }

  private void getAlbumReviews(String albumId, HttpServletResponse response) throws IOException {
    try (PreparedStatement stmt = readConnection.prepareStatement(
        "SELECT review_type, COUNT(*) as count FROM album_reviews WHERE album_id = ? GROUP BY review_type")) {
      stmt.setInt(1, Integer.parseInt(albumId));
      ResultSet rs = stmt.executeQuery();

      int likes = 0, dislikes = 0;
      while (rs.next()) {
        if ("like".equals(rs.getString("review_type"))) {
          likes = rs.getInt("count");
        } else if ("dislike".equals(rs.getString("review_type"))) {
          dislikes = rs.getInt("count");
        }
      }

      // Return JSON response
      String jsonResponse = "{\"albumId\": " + albumId + ", \"likes\": " + likes + ", \"dislikes\": " + dislikes + "}";
      response.getWriter().write(jsonResponse);

    } catch (SQLException e) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("{\"error\": \"Database error: " + e.getMessage() + "\"}");
    }
  }


  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String url = request.getPathInfo();
    if (url == null || url.isEmpty()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("missing parameters");
      return;
    }

    if (url.equals("/IGORTON/AlbumStore/1.0.0/albums")) {
      handleAlbumUpload(request, response);
    } else if (url.startsWith("/review/")) {
      handleLikeDislike(request, response);
    } else {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("invalid parameters");
    }
  }

  private void handleAlbumUpload(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String artist = "", title = "", yearStr = "";
    byte[] imageData = null;
    long imageSize = 0;

    for (Part p : request.getParts()) {
      if (p.getName().equals("image")) {
        try (InputStream s = p.getInputStream()) {
          imageData = s.readAllBytes();
          imageSize = imageData.length;
        }
      }
      if (p.getName().equals("profile")) {  // Match the exact name in the client
        String profileJson = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        JsonObject jsonObject = JsonParser.parseString(profileJson).getAsJsonObject();
        artist = jsonObject.get("artist").getAsString();
        title = jsonObject.get("title").getAsString();
        yearStr = jsonObject.get("year").getAsString();
      }
    }

    //Debug
    if (yearStr == null || yearStr.trim().isEmpty()) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getWriter().write("Invalid or missing 'year' field.");
      return;
    }

    try {
      PreparedStatement stmt = writeConnection.prepareStatement(
          "INSERT INTO albums (artist, title, year, image, image_size) VALUES (?, ?, ?, ?, ?)",
          Statement.RETURN_GENERATED_KEYS
      );
      stmt.setString(1, artist);
      stmt.setString(2, title);
      stmt.setInt(3, Integer.parseInt(yearStr));
      stmt.setBytes(4, imageData);
      stmt.setLong(5, imageSize);
      stmt.executeUpdate();

      ResultSet generatedKeys = stmt.getGeneratedKeys();
      int albumId = generatedKeys.next() ? generatedKeys.getInt(1) : -1;

      response.setStatus(HttpServletResponse.SC_CREATED);
      response.getWriter().write(new Gson().toJson(new Album(artist, title, Integer.parseInt(yearStr), albumId)));
    } catch (SQLException e) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("Database error: " + e.getMessage());
    }
  }

  private void handleLikeDislike(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String[] parts = request.getPathInfo().split("/");
    if (parts.length != 4) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getWriter().write("Invalid like/dislike request format.");
      return;
    }

    String reviewType = parts[2]; // like or dislike
    int albumId = Integer.parseInt(parts[3]);

    try {
      // Publish to RabbitMQ
      publishToQueue(albumId, reviewType);
      response.setStatus(HttpServletResponse.SC_CREATED);
      response.getWriter().write("Review submitted.");
    } catch (Exception e) {
      e.printStackTrace();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("Failed to publish review." + e.getMessage());
    }
  }

  /**
   * Publish a post review query to RabbitMQ queue
   * @param albumId
   * @param reviewType
   * @throws Exception
   */
  private void publishToQueue(int albumId, String reviewType) throws Exception {
    Channel channel = channelPool.take();
    try {
      String message = albumId + "," + reviewType;
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
    } finally {
      channelPool.offer(channel);
    }
  }

  @Override
  public void destroy() {
    try {
      if (writeConnection != null) writeConnection.close();
      if (readConnection != null) readConnection.close();
      if (mqConnection != null) mqConnection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
