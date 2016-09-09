package com.dummystreams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Stream;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.qbx.ctavo.dto.DeviceData;

public class MassPublisher {

	private ArrayList<String> deviceUIDs;
	private static MqttClient mqttClient;
	public static ObjectMapper mapper = new ObjectMapper();
	private static Connection connection;
	private static Session session;
	private static HashMap<String, MessageProducer> publishers = new HashMap<String, MessageProducer>();
	private static Topic topic;

	public MassPublisher() {

	}

	public static void main(String[] argv) throws Exception {
		if (argv.length == 3) {
			/*
			 * mqttClient = new MqttClient(
			 * "tcp://ec2-52-58-90-188.eu-central-1.compute.amazonaws.com:1883",
			 * MqttClient.generateClientId()+"asad-kazmi");
			 * mqttClient.setTimeToWait(10000);
			 * 
			 * MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
			 * mqttConnectOptions.setUserName("admin");
			 * mqttConnectOptions.setPassword("admin".toCharArray());
			 * 
			 * mqttClient.connect(mqttConnectOptions);
			 * 
			 * MassPublisher m = new MassPublisher();
			 * m.publish(Integer.parseInt(argv[0]));
			 */
			Integer noOfDevices = Integer.parseInt(argv[0]);
			double scaleFactor = Double.parseDouble(argv[1]);
			double meanFactor = Double.parseDouble(argv[2]);
			// String url =
			// "failover:(tcp://ec2-52-59-100-96.eu-central-1.compute.amazonaws.com:61616)?initialReconnectDelay=100&maxReconnectDelay=300";
			// // new test broker
			// String url =
			// "failover:(tcp://ec2-52-59-38-125.eu-central-1.compute.amazonaws.com:61616)?initialReconnectDelay=100&maxReconnectDelay=300";
			// // new prod broker
			String url = "tcp://localhost:61616";

			ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("admin", "admin", url);
			connection = factory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			for (int i = 1; i <= noOfDevices; i++) {
				String topicName = "connectavo.devices.4000002707881.191238";
				// topic = session.createTopic("connectavo.devices."+i);
				topic = session.createTopic(topicName);

				// publishers.put("connectavo.devices."+i,
				// session.createProducer(topic));
				// publishers.get("connectavo.devices."+i).setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				publishers.put(topicName, session.createProducer(topic));
				publishers.get(topicName).setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			}

			MassPublisher m = new MassPublisher();
			m.publish(noOfDevices, scaleFactor, meanFactor);
		} else {
			System.out.println("Invalid no. of arguments to Main class...");
			System.out.println("Enter params as : no. of Devices, scaleFactor, meanFactor");
		}

	}

	public void run(Integer noOfDevices, double scaleFactor, double meanFactor) throws JMSException {
//		String url = "failover:(tcp://ec2-52-59-100-96.eu-central-1.compute.amazonaws.com:61616)?initialReconnectDelay=100&maxReconnectDelay=300"; // new test broker
//		String url = "failover:(tcp://ec2-52-59-38-125.eu-central-1.compute.amazonaws.com:61616)?initialReconnectDelay=100&maxReconnectDelay=300"; // new prod broker
		String url = "tcp://localhost:61616";

		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("admin", "admin", url);
		connection = factory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		for (int i = 1; i <= noOfDevices; i++) {
			String topicName = "connectavo.devices.4000002707881.191238";
			// topic = session.createTopic("connectavo.devices."+i);
			topic = session.createTopic(topicName);

			// publishers.put("connectavo.devices."+i,
			// session.createProducer(topic));
			// publishers.get("connectavo.devices."+i).setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			publishers.put(topicName, session.createProducer(topic));
			publishers.get(topicName).setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		}

		publish(noOfDevices, scaleFactor, meanFactor);
	}

	private void publish(int noOfDevices, double scaleFactor, double meanValue) {
		this.setDeviceUIDs(noOfDevices);
		while (true) {
			Stream<String> a = deviceUIDs.parallelStream();
			System.out.println("parallelstream id is : "+a);
			deviceUIDs.parallelStream().forEach(u -> {
				DeviceData deviceData = new DeviceData();
				// deviceData.setDeviceUid(u);
				String deviceUid = "4000002707881.191238";
				deviceData.setDeviceUid(deviceUid);
				deviceData.setTimestamp(System.currentTimeMillis() / 1000);
				HashMap<String, String> data = new HashMap<String, String>();
				Integer distance = 291;
				Random r = new Random();
				distance = (int) Math.round((r.nextGaussian() * scaleFactor + meanValue));
				// System.out.println("Distance : " + distance);
				data.put("distance", distance.toString());
				deviceData.setData(data);
				String deviceDataString;
				try {
					deviceDataString = mapper.writeValueAsString(deviceData);
					System.out.println("Device Data: " + deviceUid + " " + deviceDataString);
					/*
					 * MqttMessage message = new MqttMessage();
					 * message.setPayload(deviceDataString.getBytes());
					 * message.setRetained(false);
					 * mqttClient.publish("connectavo.devices."+deviceData.
					 * getDeviceUid(), message);
					 */
					TextMessage message = session.createTextMessage(deviceDataString);
					// publishers.get("connectavo.devices."+u).send(message);
					publishers.get("connectavo.devices." + deviceUid).send(message);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			;
			try {
				Thread.sleep(1000);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	public ArrayList<String> getDeviceUIDs() {
		return deviceUIDs;
	}

	public void setDeviceUIDs(int max) {
		deviceUIDs = new ArrayList<String>();
		for (int i = 1; i <= max; i++) {
			deviceUIDs.add(String.valueOf(i));
		}
	}

}
