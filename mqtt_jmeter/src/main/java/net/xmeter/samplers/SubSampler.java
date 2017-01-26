package net.xmeter.samplers;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SubSampler extends AbstractMQTTSampler implements ThreadListener {
	//private transient MQTT mqtt = new MQTT();
	private transient MqttClient mqtt = null;
	//private transient CallbackConnection connection = null;
	private transient static Logger logger = LoggingManager.getLoggerForClass();

	private boolean connectFailed = false;
	private boolean subFailed = false;
	private boolean receivedMsgFailed = false;
	private CountDownLatch latch = new CountDownLatch(1);

//	private int receivedMessageSize = 0;
	private int receivedCount = 0;
	private double avgElapsedTime = 0f;

	private List<String> contents = new ArrayList<String>();

	private Object lock = new Object();
	
	private boolean initialized = false;

	//private int qos = QOS_0;
	/**
	 * 
	 */
	private static final long serialVersionUID = 2979978053740194951L;

	public int getQOS() {
		return getPropertyAsInt(QOS_LEVEL, QOS_0);
	}

	public void setQOS(int qos) {
		setProperty(QOS_LEVEL, qos);
	}

	public String getTopic() {
		return getPropertyAsString(TOPIC_NAME, DEFAULT_TOPIC_NAME);
	}

	public void setTopic(String topicName) {
		setProperty(TOPIC_NAME, topicName);
	}

	public boolean isAddTimestamp() {
		return getPropertyAsBoolean(ADD_TIMESTAMP);
	}

	public void setAddTimestamp(boolean addTimestamp) {
		setProperty(ADD_TIMESTAMP, addTimestamp);
	}

	public boolean isDebugResponse() {
		return getPropertyAsBoolean(DEBUG_RESPONSE, false);
	}

	public void setDebugResponse(boolean debugResponse) {
		setProperty(DEBUG_RESPONSE, debugResponse);
	}

	public String getConnPrefix() {
		return getPropertyAsString(CONN_CLIENT_ID_PREFIX, DEFAULT_CONN_PREFIX_FOR_SUB);
	}

	public SampleResult sample(Entry arg0) {
		SampleResult result = new SampleResult();
		
		try {
			if (!initialized)
				initialize();
		} catch (Exception e) {
			result.setResponseMessage(e.getMessage());
			result.setSuccessful(false);
            return result;
		}

		long startTime = System.currentTimeMillis();
		result.sampleStart();

		if (connectFailed) {
			return fillFailedResult(result, MessageFormat.format("Connection {0} connected failed.", mqtt.toString()));
		} else if (subFailed) {
			return fillFailedResult(result, "Failed to subscribe to topic.");
		} else if (receivedMsgFailed) {
			return fillFailedResult(result, "Failed to receive message.");
		}
		
        synchronized (this) {
            try {
				wait();
			} catch (InterruptedException e) {
				logger.info(e.toString());
			}
        }

		synchronized (lock) {
			String message = MessageFormat.format("Received {0} of message\n.", receivedCount);
			StringBuffer content = new StringBuffer("");
			if (isDebugResponse()) {
				for (int i = 0; i < contents.size(); i++) {
					content.append(contents.get(i) + " \n");
				}
			}
			int avgSize = 0;
			if (receivedCount != 0) {
				//avgSize = receivedMessageSize / receivedCount;
				avgSize = receivedCount;
			}
			result = fillOKResult(result, avgSize, message, content.toString());
			if (isAddTimestamp()) {
				result.setEndTime(startTime + (long) this.avgElapsedTime);
			}
			result.setSampleCount(receivedCount);

			//receivedMessageSize = 0;
			receivedCount = 0;
			avgElapsedTime = 0f;
			contents.clear();

			return result;
		}
	}

	private SampleResult fillFailedResult(SampleResult result, String message) {
		result.sampleEnd();
		result.setThreadName("MQTT Pub Sampler");
		result.setResponseCode("500");
		result.setSuccessful(false);
		result.setResponseMessage(message);
		result.setResponseData("Failed.".getBytes());
		return result;
	}

	private SampleResult fillOKResult(SampleResult result, int size, String message, String contents) {
		result.sampleEnd();
		result.setThreadName("MQTT Pub Sampler");
		result.setResponseCode("200");
		result.setSuccessful(true);
		result.setResponseMessage(message);
		result.setBodySize(size);
		result.setBytes(size);
		result.setResponseData(contents.getBytes());
		return result;
	}

	public void threadFinished() {
		try {
			notifyAll();
			if (mqtt.isConnected())
				mqtt.disconnect(DEFAULT_CONN_TIME_OUT);
			logger.info(MessageFormat.format("Connection {0} disconnect successfully.", mqtt));
		} catch (MqttException e) {
			logger.info(MessageFormat.format("Connection {0} failed to disconnect.", mqtt));
		}
	}

	private void initialize() throws Exception {
		String serverURI = getProtocol().toLowerCase() + "://" + getServer() + ":" + getPort();
		String clientId = getConnPrefix();
		mqtt = new MqttClient(serverURI, clientId, new MemoryPersistence());

		MqttConnectOptions options = new MqttConnectOptions();

		options.setKeepAliveInterval((short) getConnKeepAlive());
		if (getConnReconnAttamptMax() > 0) {
			options.setAutomaticReconnect(true);
		}

		if (!"".equals(getUserNameAuth().trim())) {
			options.setUserName(getUserNameAuth());
		}
		if (!"".equals(getPasswordAuth().trim())) {
			options.setPassword(getPasswordAuth().toCharArray());
		}

		mqtt.setCallback(new MqttCallback() {
			@Override
			public void connectionLost(Throwable cause) {
				logger.log(Priority.ERROR, cause.getMessage(), cause);
				connectFailed = true;
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				logger.log(Priority.ERROR, "token is : " + (token.isComplete() ? "true" : "false"));
			}

			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				logger.log(Priority.INFO, MessageFormat.format("Topic: {0}, Revice Message: {1} .", topic,
						new String(message.getPayload())));
				contents.add(new String(message.getPayload()));
				receivedCount++;
			}
		});

		MqttTopic topic = mqtt.getTopic(this.getTopic());
		options.setWill(topic, "close".getBytes(), 0, true);
		mqtt.connect(options);
		mqtt.subscribe(this.getTopic(), getQOS());

		initialized = true;
	}

	@Override
	public void threadStarted() {
		
	}


}
