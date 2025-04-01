package org.yinuo.rabbitmq101;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class MQConsumerInitializer implements ServletContextListener {

  private Thread consumerThread;

  @Override
  public void contextInitialized(ServletContextEvent event) {
    consumerThread = new Thread(() -> {
      try {
        MQConsumer.startConsuming();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    consumerThread.setDaemon(true);
    consumerThread.start();
    System.out.println("🚀 MQ Consumer started.");
  }

  @Override
  public void contextDestroyed(ServletContextEvent event) {
    consumerThread.interrupt();
    System.out.println("🛑 MQ Consumer stopped.");
  }
}
