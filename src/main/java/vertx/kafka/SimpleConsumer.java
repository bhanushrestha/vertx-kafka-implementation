package vertx.kafka;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

	private final ConsumerConnector consumerConnector;
	
	
	private Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

	public SimpleConsumer(String a_zookeeper, String a_groupId)
	{
		
		consumerConnector = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
						a_groupId));
		logger.info("Consumer started with group id: {}",a_groupId);
	}

	
	public ConsumerConnector getConsumerConnector(){
		return consumerConnector;
	}
	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	
	public void kill1() {
        if (consumerConnector != null) consumerConnector.shutdown();
      
   }
	
	
 
}