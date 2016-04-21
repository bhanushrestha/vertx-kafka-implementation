package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.I0Itec.zkclient.exception.ZkTimeoutException;

import vertx.kafka.KafkaProducer;
import vertx.kafka.SimpleConsumer;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class RestMain extends AbstractVerticle {

	// Convenience method so you can run it in your IDE
	public static void main(String[] args) {
		Runner.runExample(RestMain.class);
	}

	private Map<String, JsonObject> products = new HashMap<>();

	@Override
	public void start() {

		VertxOptions options = new VertxOptions();
		options.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
		vertx = Vertx.vertx(options);
		Router router = Router.router(vertx);

		router.route().handler(BodyHandler.create());
		router.get("/hello").handler(this::hello);

		vertx.createHttpServer().requestHandler(router::accept).listen(8080);
	}

	private void sendError(int statusCode, HttpServerResponse response) {
		response.setStatusCode(statusCode).end();
	}

	private void hello(RoutingContext routingContext) {

		KafkaProducer kp = new KafkaProducer();
		System.out.println("Hello");
		
/*
 * 		JSONObject sent to Kafka topic container contains 
 * 		docker image name,tag and name of container
 *		For this example we want the other side to 
 *		start docker image called python with tag latest 
*/
		JsonObject json = new JsonObject();
		json.put("imagename", "python");
		json.put("imagetag", "latest");
		json.put("containername", "contan");
		json.put("process", "START");

		// Here the second argument decides which topic we send the message to
		kp.produce(json.toString(), "container");

		HttpServerResponse response = routingContext.response();
		try {
			response.putHeader("content-type", "application/json").end(
					consumer());
		} catch (VertxException | ZkTimeoutException e) {
			response.putHeader("content-type", "application/json").end(
					"Try again!");
		}

	}

	private String consumer() {

		Integer i = new Random().nextInt(4);
		SimpleConsumer sc = new SimpleConsumer("localhost" + ":2181", "random"
				+ i);
		ConsumerConnector consumerConnector = sc.getConsumerConnector();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		System.out.println(new Integer(1));
		System.out.println(new Integer(1));
		topicCountMap.put("hello", new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get("hello").get(0);// remove
																				// args
																				// if
																				// error
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			String newString = new String(it.next().message());
			System.out.println(newString);
			if (!newString.equals("hello1") && !newString.equals("hello2")) {
				System.out.println(">>" + newString);
				consumerConnector.shutdown();
				return newString;
			}

		}

		return null;
	}

}