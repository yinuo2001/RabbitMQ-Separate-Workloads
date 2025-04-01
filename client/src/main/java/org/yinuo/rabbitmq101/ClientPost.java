package org.yinuo.rabbitmq101;

import com.codahale.metrics.MetricAttribute;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.HttpMultipartMode;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class ClientPost implements Runnable {
  private static AtomicInteger successCount = new AtomicInteger(0);
  private static AtomicInteger failCount = new AtomicInteger(0);
  private String postAlbumUrl;
  private String postReviewUrl;
  private CloseableHttpClient client;
  private List<Row> data;
  private File file;

  public ClientPost(String IPAddr, CloseableHttpClient client, List<Row> data, File file)
      throws IOException, TimeoutException {
    this.postAlbumUrl = "http://" + IPAddr + "/IGORTON/AlbumStore/1.0.0/albums";
    this.postReviewUrl = "http://" + IPAddr + "/review";
    this.client = client;
    this.data = data;
    this.file = file;
  }

  // stolen from https://hc.apache.org/httpclient-legacy/tutorial.html
  public void run() {
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.STRICT);
    // 1. Add the file part
    builder.addBinaryBody(
        "image",
        file,
        ContentType.IMAGE_JPEG,
        "Example.jpg"
    );
    // 2. Add the profile field as a single JSON string
    // Example JSON: {"artist":"AgustD","title":"D-Day","year":"2023"}
    // Modify these values or pass them in as parameters if needed
    String jsonProfile = "{\"artist\":\"AgustD\",\"title\":\"D-Day\",\"year\":\"2023\"}";

    // Add the 'profile' field with JSON content
    builder.addTextBody("profile", jsonProfile, ContentType.APPLICATION_JSON);

    HttpEntity entity = builder.build();

    // Create a post method instance.
    HttpPost postMethod = new HttpPost(postAlbumUrl);

    try {
      postMethod.setEntity(entity);
      long start = System.currentTimeMillis();
      CloseableHttpResponse response = client.execute(postMethod);
      //postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));

      int statusCode = response.getCode();
      // Distinguish success vs failure
      if (statusCode >= 200 && statusCode < 300) {
        successCount.incrementAndGet();
      } else {
        failCount.incrementAndGet();
        // print error message
        System.err.println("Post Method failed: " + statusCode);
      }

      long end = System.currentTimeMillis();

      long latency = end - start;
      data.add(RowFactory.create(start, "POST", latency, statusCode));

      // Consume response content
      EntityUtils.consume(response.getEntity());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void postThreeReviews() {
    postReview(1, "like");
    postReview(1, "like");
    postReview(1, "dislike");
  }

  // Post reviews that indicate a like/dislike for the album
  public void postReview(int albumId, String like) {
    HttpPost postMethod = new HttpPost(postReviewUrl + "/" + like + "/" + albumId);
    long startTime = System.currentTimeMillis();
    try {
      CloseableHttpResponse response = client.execute(postMethod);
      int statusCode = response.getCode();
      if (statusCode >= 200 && statusCode < 300) {
        //System.err.println("Post Method success: " + statusCode);
        successCount.incrementAndGet();
        long endTime = System.currentTimeMillis();
        long latency = endTime - startTime;
        data.add(RowFactory.create(startTime, "POST", latency, statusCode));
      } else {
        failCount.incrementAndGet();
      }
      EntityUtils.consume(response.getEntity());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public static int getSuccessCount() {
    return successCount.get();
  }

  public static int getFailCount() {
    return failCount.get();
  }
}
