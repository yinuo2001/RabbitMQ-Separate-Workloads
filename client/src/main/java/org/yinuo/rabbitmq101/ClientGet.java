package org.yinuo.rabbitmq101;

import java.io.IOException;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class ClientGet implements Runnable {
  private static AtomicInteger successCount = new AtomicInteger(0);
  private static AtomicInteger failCount = new AtomicInteger(0);

  private String getUrl;
  private CloseableHttpClient client;
  private List<Row> data;

  public ClientGet(String IPAddr, CloseableHttpClient client, List<Row> data) {
    this.getUrl = "http://" + IPAddr + "/album/1";
    this.client = client;
    this.data = data;
  }

  // stolen from https://hc.apache.org/httpclient-legacy/tutorial.html
  public void run() {

    // Create a method instance.
    HttpGet getMethod = new HttpGet(getUrl);

    // Create a post method instance.

    // Provide custom retry handler is necessary
    /*
    getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(5, false));
     */
    try {
      long start = System.currentTimeMillis();
      System.out.println("GET START: " + start + Thread.currentThread().getName());
      // Execute the method.
      CloseableHttpResponse response = client.execute(getMethod);

      int statusCode = response.getCode();

      if (statusCode >= 200 && statusCode < 300) {
        successCount.incrementAndGet();
      } else {
        failCount.incrementAndGet();

        System.err.println("Post Method failed: " + statusCode);
      }

      // Read the response body.
      byte[] responseBody = response.getEntity().getContent().readAllBytes();
      //System.out.println(new String(responseBody));
      long end = System.currentTimeMillis();
      System.out.println("GET END: " + end + Thread.currentThread().getName());
      long latency = end - start;
      data.add(RowFactory.create(start, "GET", latency, statusCode));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static int getSuccessCount() {
    return successCount.get();
  }

  public static int getFailCount() {
    return failCount.get();
  }
}
