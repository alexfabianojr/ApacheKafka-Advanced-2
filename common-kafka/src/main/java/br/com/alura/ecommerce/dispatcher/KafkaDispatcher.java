package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        return send(topic, key, id, payload);
    }

    public void sendSync(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = send(topic, key, id, payload);
        future.get();
    }

    private Future<RecordMetadata> send(String topic, String key, CorrelationId id, T payload) {
        var value = new Message<T>(id.continueWith("_" + topic), payload);
        var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() +
                    ":::partition " + data.partition() +
                    "/ offset " + data.offset() +
                    "/ timestamp " + data.timestamp());
        };
        var future = producer.send(record, callback);
        return future;
    }

    @Override
    public void close() {
        producer.close();
    }
}
