package org.yinuo.rabbitmq101;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class ClientGetReview {
  private String getReviewURL;
  private CloseableHttpClient client;
  private List<Row> data;
  private static AtomicInteger successCount = new AtomicInteger(0);
  private static AtomicInteger failCount = new AtomicInteger(0);

  public ClientGetReview(String IPAddress, CloseableHttpClient client, List<Row> data) {
    // Only get reviews for album no.1
    this.getReviewURL = "http://" + IPAddress + "/reviews/1/reviews";
    this.client = client;
    this.data = data;
  }

  public void run() {
    long startTime = System.currentTimeMillis();
    HttpGet get = new HttpGet(getReviewURL);

    try {
      CloseableHttpResponse response = client.execute(get);
      int statusCode = response.getCode();
      if (statusCode >= 200 && statusCode < 300) {
        successCount.incrementAndGet();
      } else {
        failCount.incrementAndGet();
      }

      long endTime = System.currentTimeMillis();
      long latency = endTime - startTime;
      data.add(RowFactory.create(startTime, "GET", latency, statusCode));
      EntityUtils.consume(response.getEntity());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static AtomicInteger getSuccessCount() {
    return successCount;
  }

  public static AtomicInteger getFailCount() {
    return failCount;
  }
}
