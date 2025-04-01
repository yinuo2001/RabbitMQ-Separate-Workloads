package org.yinuo.rabbitmq101;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.TimeValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

// Delete data.csv folder each time before running the program
public class Main {
  private static final int GROUP_SIZE = 10;
  private static final int NUMBER_OF_GROUPS = 30;

  private static final int NUM_GET_REVIEWS = 3;

  private static volatile boolean keepRunning = true;

  // IP address of the java servlet server
  private static final String IPAddr = "35.91.187.149:8080/server_war";
  private static final String FILE_PATH = "/Users/elise/Desktop/RabbitmqImplementation/client/src/main/resources/Example.jpg";

  public static void main(String[] args) {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(1000);
    connectionManager.setDefaultMaxPerRoute(1000);

    CloseableHttpClient client = HttpClients.custom().setConnectionManager(connectionManager)
        .setRetryStrategy(new DefaultHttpRequestRetryStrategy(5, TimeValue.ofSeconds(2))).build();

    try {
      SparkSession spark =
          SparkSession.builder().appName("data").config("spark.master", "local").getOrCreate();
      spark.sparkContext().setLogLevel("ERROR");
      StructType schema =
          new StructType().add("start", "long").add("requestType", "string").add("latency", "long")
              .add("code", "integer");
      List<Row> list = Collections.synchronizedList(new ArrayList<>());
      File file = new File(FILE_PATH);
      //ClientGet clientGet = new ClientGet(IPAddr, client, list);
      ClientPost clientPost = new ClientPost(IPAddr, client, list, file);
      ClientGetReview clientGetReview = new ClientGetReview(IPAddr, client, list);

      int totalThreads = GROUP_SIZE * NUMBER_OF_GROUPS;
      CountDownLatch completed = new CountDownLatch(totalThreads);

      // Prepare for get reviews
      List<Thread> getThreads = new ArrayList<>();
      CountDownLatch firstGroupLatch = new CountDownLatch(GROUP_SIZE);

      long start = System.currentTimeMillis();
      for (int j = 0; j < NUMBER_OF_GROUPS; j++) {
        for (int i = 0; i < GROUP_SIZE; i++) {
          int finalJ = j;
          Runnable thread = () -> {
            for (int k = 0; k < 100; k++) {
              clientPost.postThreeReviews();
            }
            if (finalJ == 0) {
              firstGroupLatch.countDown();
            }
            completed.countDown();
          };
          new Thread(thread).start();
        }
      }

      // Get reviews
      firstGroupLatch.await();
      long getStart = System.currentTimeMillis();
      for (int i = 0; i < NUM_GET_REVIEWS; i++) {
        Thread t = new Thread(() -> {
          while (keepRunning) {
            clientGetReview.run();
          }
        });
        getThreads.add(t);
        t.start();
      }

      completed.await();
      keepRunning = false;
      long getEnd = System.currentTimeMillis();

      for (Thread t : getThreads) {
        t.join();
      }

      long end = System.currentTimeMillis();
      Dataset<Row> data = spark.createDataFrame(list, schema);

      Dataset<Row> getMean = data.filter("requestType == 'GET'").groupBy().avg("latency");
//      Dataset<Row> postMean = data.groupBy().avg("latency");
      Dataset<Row> getMin = data.filter("requestType == 'GET'").groupBy().min("latency");
//      Dataset<Row> postMin = data.groupBy().min("latency");
      Dataset<Row> getMax = data.filter("requestType == 'GET'").groupBy().max("latency");
//      Dataset<Row> postMax = data.groupBy().max("latency");
      double[] get5099 = data.filter("requestType == 'GET'").stat().approxQuantile("latency", new double[] {0.5, 0.99}, 0);
      //double[] post5099 = data.stat().approxQuantile("latency", new double[] {0.5, 0.99}, 0);
      System.out.println("GET Mean Latency: " + getMean.first().getDouble(0));
//      System.out.println("Mean Latency: " + postMean.first().getDouble(0));
      System.out.println("GET Min Latency: " + getMin.first().getLong(0));
//      System.out.println("Min Latency: " + postMin.first().getLong(0));
      System.out.println("GET Max Latency: " + getMax.first().getLong(0));
//      System.out.println("Max Latency: " + postMax.first().getLong(0));
      System.out.println("GET 50th Percentile: " + get5099[0]);
//      System.out.println("50th Percentile: " + post5099[0]);
      System.out.println("GET 99th Percentile: " + get5099[1]);
//      System.out.println("99th Percentile: " + post5099[1]);
      data.write().format("csv").save("data.csv");
      spark.stop();

      int getReviewSuccesses = ClientGetReview.getSuccessCount().get();
      int getReviewFails = ClientGetReview.getFailCount().get();
      int totalGetRequests = getReviewSuccesses + getReviewFails;
      double getThroughput = totalGetRequests * 1000.0 / (getEnd - getStart);

      int postSuccesses = ClientPost.getSuccessCount();
      int postFails = ClientPost.getFailCount();
      int totalPostRequests = postSuccesses + postFails;
      double postThroughput = totalPostRequests * 1000.0 / (end - start);

      System.out.println("Number of successful GET review requests: " + getReviewSuccesses);
      System.out.println("Number of failed GET review requests: " + getReviewFails);

      System.out.println("Number of successful POST requests: " + postSuccesses);
      System.out.println("Number of failed POST requests: " + postFails);

      System.out.println("Time taken: " + (end - start) / 1000 + "s");
      System.out.println("Get throughput: " + getThroughput + " requests/s");
      System.out.println("Post throughput: " + postThroughput + " requests/s");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}