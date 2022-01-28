package ProjectGenerator;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SchemaRegistry {
    public static void main(String args[]) {

        Properties emp = new Properties();
        emp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        emp.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka avro Serializer
        emp.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        //the schema
        emp.setProperty("schema.registry.url", "http://localhost:8081");

        Employee employee = new Employee("Chaitanya", 26);

        KafkaProducer<String, Employee> producer = new KafkaProducer<String, Employee>(emp);
        ProducerRecord<String, Employee> producerRecord = new ProducerRecord<String, Employee>("company", employee);
        System.out.println(employee);

        //send the data to kafka topic
        producer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();

    }
}