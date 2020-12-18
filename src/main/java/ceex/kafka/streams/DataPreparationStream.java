package ceex.kafka.streams;

// The basic structure of this code is adapted from the examples in Stephane Maarek's Udemy course:
// https://www.udemy.com/course/kafka-streams/

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

// import the following when using Kafka Streams state stores
//import org.apache.kafka.streams.processor.ProcessorContext;
//import org.apache.kafka.streams.state.KeyValueStore;
//import org.apache.kafka.streams.state.StoreBuilder;
//import org.apache.kafka.streams.state.Stores;


public class DataPreparationStream {

    public static void main(String[] args) {

        // define key and value serializer and deserializer (serde)
        // key = String
        // value = Avro (GenericRecord)
        Serde<GenericRecord> value_serde = new GenericAvroSerde();
        value_serde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081/"),
                false);

        Serde<String> key_serde = Serdes.String();

        // set stream properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "data-preparation-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // AUTO_OFFSET_RESET_CONFIG = earliest --> used during development, change this according to your needs

        // add interceptors to properties for performance monitoring
        properties.put(
                StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        properties.put(
                StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        // create stream builder
        StreamsBuilder builder = new StreamsBuilder();

        // define the different state stores
        // IMPORTANT: only use Kafka Streams state stores in production
        // data will be lost in case of system interruption when using HashMaps or other simple variables
        Map<String, Integer> dealNrPerNode = new HashMap<String, Integer>();
        Map<String, GenericRecord> lastValues = new HashMap<String, GenericRecord>();
        Map<java.lang.Long, List<String>> keys = new HashMap<Long, List<String>>();
        Long[] lastTimestamp = {null};
        Long[] tempTimestamp = {null};

        // create input stream with above defined key value serde
        // K_STREAMS_PRICES_V2 = source topic
        KStream<String, GenericRecord> inputStream = builder.stream("K_STREAMS_PRICES_V2", Consumed.with(key_serde, value_serde));

        // create output stream from input stream
        // flatMap is used here as the return value consists of a list with potentially multiple messages (although the list often just contains one element)
        KStream<String, GenericRecord> outputStream = inputStream.flatMap((KeyValueMapper<String, GenericRecord, Iterable<KeyValue<String, GenericRecord>>>) (key, value) -> {

            // set deal number to one if there is no deal for the current message (based on key)
            dealNrPerNode.putIfAbsent(key, 1);
            // get deal number for current message
            Integer deal = dealNrPerNode.get(key);

            // initiate return value
            List<KeyValue<String, GenericRecord>> returnValue = new ArrayList<>();

            // keys is used to compare the lists of message with the same timestamp
            // the timestamp is the key of the HashMap and the value consists of a list of keys (from messages) belonging to the respective timestamp
            // if there is no entry with the timestamp of the current message, create a new entry and add the key of the current message to the value
            if (keys.get((long) value.get("TIME")) == null) {
                keys.put((long) value.get("TIME"), new ArrayList<>());
                keys.get((long) value.get("TIME")).add(key);
            }
            // if there is already an entry with the respective timestamp, just add the key of the current message to the value list
            else {
                keys.get((long) value.get("TIME")).add(key);
            }

            // check if the timestamp of the current message is different to the one of the last message
            if (tempTimestamp[0] != null && (long) value.get("TIME") != tempTimestamp[0]) {
                // if yes, check if the lists of messages from the last two timestamps are different
                if (lastTimestamp[0] != null && !keys.get(tempTimestamp[0]).equals(keys.get(lastTimestamp[0]))) {
                    // extract all message keys that appeared not with the last timestamp, but with the one before
                    // these are the deals that need to be closed

                    List<String> closingPrices = keys.get(lastTimestamp[0]);
                    closingPrices.removeAll(keys.get(tempTimestamp[0]));

                    if (closingPrices.size() > 0) {
                        // extract the respective messages from lastValues, update end_time and deal and...
                        // add the messages to the list of return values
                        for (String closingKey : closingPrices) {
                            GenericRecord closingValue = lastValues.get(closingKey);

                            // remove key from lastValues, as for the next time the key appears a new deal should be opened
                            lastValues.remove(closingKey);

                            //closingValue.put("END_TIME", value.get("TIME"));
                            closingValue.put("END_TIME", tempTimestamp[0]);
                            closingValue.put("DEAL", dealNrPerNode.get(closingKey));

                            returnValue.add(new KeyValue<>(closingKey, closingValue));
                            // increment deal number for the respective key
                            dealNrPerNode.put(closingKey, dealNrPerNode.get(closingKey)+1);
                        }
                    }
                }

                // update timestamp stores
                lastTimestamp[0] = tempTimestamp[0];

            }

            // check if there is a message with the same key as the current in lastValues
            // if yes, extract it from lastValues
            if (lastValues.get(key) != null) {
                GenericRecord prevValue = lastValues.get(key);

                // if the price changed, update deal of both the current and the previous message...
                // and update end_time of the previous message with the timestamp of the current one
                if (!prevValue.get("PRICE").equals(value.get("PRICE"))) {
                    prevValue.put("END_TIME", (long) value.get("TIME"));
                    prevValue.put("DEAL", deal);
                    value.put("DEAL", deal);

                    // update lastValues with the current message
                    lastValues.put(key, value);

                    // add previous message (with updated attributes) to the list of return values
                    returnValue.add(new KeyValue<>(key, prevValue));
                }

                // if the price did not change, just update deal of the current message and add it to the return values
                else {
                    value.put("DEAL", deal);
                    returnValue.add(new KeyValue<>(key, value));
                }

            }

            // if there is no previous message with the same key in lastValues, update deal of the current message...
            // add it to lastValues and add it to the list of return values
            else {
                value.put("DEAL", deal);
                lastValues.put(key, value);
                returnValue.add(new KeyValue<>(key, value));
            }

            // set active timestamp to the one of the current message
            tempTimestamp[0] = (long) value.get("TIME");

            return returnValue;

        });

        // PRICES_KSTREAMS_OUTPUT_V2 = target topic
        outputStream.to("PRICES_KSTREAMS_OUTPUT_V2", Produced.with(key_serde, value_serde));

        // build and start stream
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        System.out.println(streams.toString());

        // safely close stream when shutdown command is issued
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
