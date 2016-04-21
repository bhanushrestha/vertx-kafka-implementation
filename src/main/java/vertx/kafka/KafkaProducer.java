package vertx.kafka;

import java.util.Properties;
import java.util.Random;

import javax.annotation.PostConstruct;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducer {

	private static Producer producer;
	public final Properties props = new Properties();

	private Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	
	//Enter advertised host name of the kafka server here
	private static String kafka = "localhost";
	private String brokerListPort = ":9092";

	public KafkaProducer() {
		init();
	}

	public void init() {
		props.put("metadata.broker.list", kafka + brokerListPort);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "vertx.kafka.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		setProducer(new Producer(config));
		logger.info("initialized!!!!");
	}

	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		KafkaProducer.producer = producer;
	}

	public void produce(String message, String topic) {

		Integer i = new Random().nextInt();

		KeyedMessage<String, String> data1 = new KeyedMessage<String, String>(
				topic, i.toString(), message);
		getProducer().send(data1);
	}

}
