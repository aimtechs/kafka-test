package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Consumer {

  final AtomicInteger receiveCount = new AtomicInteger(0);

  public static void main(String[] args) throws InterruptedException {

    final Consumer consumer = new Consumer();

    ContainerProperties containerProps = new ContainerProperties("topic1");
    KafkaMessageListenerContainer<Integer, String> container = consumer.createContainer(containerProps);


    container.setupMessageListener((MessageListener<Integer, String>) message -> {
      consumer.addAndGetReceiveCount();
      System.err.println( consumer.getReceiveCount() );
//        log.info("received: " + message + "\t" + consumer.addAndGetReceiveCount());
    });
    container.start();



    Scanner scanner = new Scanner(System.in);
    while (true) {
      String readed = scanner.nextLine();
      if ("stop".equals(readed)) {
        System.err.println("Consumer app will be shutdown!!!");
        break;
      }
      else if ("reset".equals(readed)) {
        consumer.resetReceiveCount();
        System.err.println("Receive Count reset.");
      }
      else if ("".equals(readed)) {
        System.err.println("");
      }
      else {
        System.err.println("Unkown command [" + readed + "]. [stop] is shutdown");
      }
    }

    container.stop();
    System.err.println("Bye~~~  Receive Count is [ " + consumer.getReceiveCount() + " ]" );
  }

  private int addAndGetReceiveCount() {
    return receiveCount.getAndIncrement();
  }

  private int getReceiveCount() {
    return receiveCount.get();
  }

  private void resetReceiveCount() {
    receiveCount.set(0);
  }

  private KafkaMessageListenerContainer<Integer, String> createContainer(ContainerProperties containerProps) {
    Map<String, Object> props = consumerProps();
    DefaultKafkaConsumerFactory<Integer, String> cf =
            new DefaultKafkaConsumerFactory<Integer, String>(props);
    KafkaMessageListenerContainer<Integer, String> container =
            new KafkaMessageListenerContainer<>(cf, containerProps);
    return container;
  }

  private Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.213:32783,192.168.100.213:32784,192.168.100.213:32785");
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group"); // server name을 붙여주면 될것이다. 하나의 topic에 대하여, load balancing이 이루어 진다.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return props;
  }
}


