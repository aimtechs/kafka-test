package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Producer {

  final int LOOP_COUNT = 20000;

  final String TOPIC = "topic1";


  public static void main(String[] args) throws Exception {

    final Producer producer = new Producer();
    System.err.println("Producer send start");

//    producer.testSimple();
    producer.testLoop();

    TimeUnit.SECONDS.sleep(3);
    System.err.println("Producer send complete");
  }

  private void testSimple() throws Exception {

    KafkaTemplate<Integer, String> template = createTemplate();

//    template.send(TOPIC, 0, "111111111111111111111111111111111111");
//    template.send(TOPIC, 1, "222222222222222222222222222222222");
//    template.send(TOPIC, 0, "33333333333333333333333333333");
//    template.send(TOPIC, 1, "4444444444444444444444444444444444444");

//    template.setDefaultTopic(TOPIC);
//    template.sendDefault(0, "111111111111111111111111111111111111");
//    template.sendDefault(2, "222222222222222222222222222222222");
//    template.sendDefault(0, "33333333333333333333333333333");
//    template.sendDefault(2, "4444444444444444444444444444444444444");

    template.flush();
  }

  private void testLoop() throws InterruptedException {
    final KafkaTemplate<Integer, String> template = createTemplate();
    template.setDefaultTopic(TOPIC);

    for (int i = 1; i <= LOOP_COUNT; i++) {
      if (i % 4 == 0) {
        template.flush();
      }
      if (i % 1000 == 0) {
        TimeUnit.SECONDS.sleep(3);
      }
      template.sendDefault(String.format("%06d", i));


      final ListenableFuture<SendResult<Integer, String>> send = template.send("aa", "bbb");

      send.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
        @Override
        public void onFailure(Throwable throwable) {

        }

        @Override
        public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
          final ProducerRecord<Integer, String> producerRecord =
                  integerStringSendResult.getProducerRecord();
          final RecordMetadata recordMetadata = integerStringSendResult.getRecordMetadata();
        }
      });
    }

    template.flush();
  }

  private KafkaTemplate<Integer, String> createTemplate() {
    Map<String, Object> senderProps = senderProps();
    ProducerFactory<Integer, String> pf =
            new DefaultKafkaProducerFactory<Integer, String>(senderProps);
    KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
    return template;
  }

  private Map<String, Object> senderProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.213:32783,192.168.100.213:32784,192.168.100.213:32785");
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }
}
