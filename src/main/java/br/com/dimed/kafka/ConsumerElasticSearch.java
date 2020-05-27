package br.com.dimed.kafka;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerElasticSearch {

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(ConsumerElasticSearch.class.getName());

        RestHighLevelClient cliente = entregaCliente();

        KafkaConsumer<String, String> consumer = criarConsumer("twitter_tweets");

        while (true){
            ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> registro : registros) {
                //Aqui iremos persistir no elastic

                IndexRequest elasticRequest = new IndexRequest("twitter").source(registro.value(), XContentType.JSON);
                IndexResponse elasticResponse = cliente.index(elasticRequest, RequestOptions.DEFAULT);
                String id = elasticResponse.getId();

                log.info("O id do documento inserido no elastic foi o "+id);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
//        cliente.close();
    }

    public static KafkaConsumer<String, String> criarConsumer(String topico){
        String servidorKafka = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";

        //Criando as propriedades essenciais
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Criando o Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topico));

        return consumer;
    }

    public static RestHighLevelClient entregaCliente(){
        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
