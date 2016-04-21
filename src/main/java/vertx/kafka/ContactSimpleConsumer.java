package vertx.kafka;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContactSimpleConsumer {

	private final ConsumerConnector consumerConnector;
	private final String topic;
	private Logger logger = LoggerFactory.getLogger(ContactSimpleConsumer.class);
	


	public ContactSimpleConsumer(String a_zookeeper, String a_groupId, String a_topic)
	{
		
		consumerConnector = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
						a_groupId));
		this.topic = a_topic;
	}

	
	public ConsumerConnector getConsumerConnector(){
		logger.info("topic is: " + topic);
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
		
		props.put("auto.offset.reset","smallest");

		return new ConsumerConfig(props);
	}

	
	public void kill1() {
        if (consumerConnector != null) consumerConnector.shutdown();
      
   }
	
	
 
}