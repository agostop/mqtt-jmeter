package net.xmeter.samplers;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import net.xmeter.Util;
import org.fusesource.mqtt.client.QoS;

public class PubSampler extends AbstractMQTTSampler implements ThreadListener {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4312341622759500786L;
	private transient static Logger logger = LoggingManager.getLoggerForClass();
	//private transient MQTT mqtt = new MQTT();
	private transient IMqttAsyncClient connection = null;
	private String payload = null;
	private String clientId = "";
	private QoS qos_enum = QoS.AT_MOST_ONCE;
	private String topicName = "";
	private String connKey = "";

	public String getQOS() {
		return getPropertyAsString(QOS_LEVEL, String.valueOf(QOS_0));
	}

	public void setQOS(String qos) {
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

	public String getMessageType() {
		return getPropertyAsString(MESSAGE_TYPE, MESSAGE_TYPE_RANDOM_STR_WITH_FIX_LEN);
	}

	public void setMessageType(String messageType) {
		setProperty(MESSAGE_TYPE, messageType);
	}

	public String getMessageLength() {
		return getPropertyAsString(MESSAGE_FIX_LENGTH, DEFAULT_MESSAGE_FIX_LENGTH);
	}

	public void setMessageLength(String length) {
		setProperty(MESSAGE_FIX_LENGTH, length);
	}

	public String getMessage() {
		return getPropertyAsString(MESSAGE_TO_BE_SENT, "");
	}

	public void setMessage(String message) {
		setProperty(MESSAGE_TO_BE_SENT, message);
	}

	public String getConnPrefix() {
		return getPropertyAsString(CONN_CLIENT_ID_PREFIX, DEFAULT_CONN_PREFIX_FOR_PUB);
	}
	
	public static byte[] hexToBinary(String hex) {
	    return DatatypeConverter.parseHexBinary(hex);
	}
	
	@Override
	public boolean isConnectionShareShow() {
		return true;
	}
	
	private String getKey() {
		String key = getThreadName();
		if(!isConnectionShare()) {
			key = new String(getThreadName() + this.hashCode());
		}
		return key;
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		this.connKey = getKey();
		if(connection == null) {
			connection = ConnectionsManager.getInstance().getConnection(connKey);
			if(connection != null) {
				logger.info("Use the shared connection: " + connection);
			} else {
				try {
					//mqtt.setVersion(getMqttVersion());
					MqttConnectOptions options = new MqttConnectOptions();
					options.setKeepAliveInterval(Integer.parseInt(getConnKeepAlive()));
					options.setMqttVersion(getMqttVersion());
					options.setConnectionTimeout(Integer.parseInt(getConnTimeout()));

					if(isClientIdSuffix()) {
						clientId = Util.generateClientId(getConnPrefix());
					} else {
						clientId = getConnPrefix();
					}
					
					options.setAutomaticReconnect(Integer.parseInt(getConnAttamptMax()) > 0);
					/*
					options.setConnectAttemptsMax(Integer.parseInt(getConlnAttamptMax()));
					options.setReconnectAttemptsMax(Integer.parseInt(getConnReconnAttamptMax()));
					*/

					if (!"".equals(getUserNameAuth().trim())) {
						options.setUserName(getUserNameAuth());
					}
					if (!"".equals(getPasswordAuth().trim())) {
						options.setPassword(getPasswordAuth().toCharArray());
					}

					Object connLock = new Object();
					MqttAsyncClient mqtt = new MqttAsyncClient(getProtocol().toLowerCase() + "://" + getServer() + ":" + getPort(), clientId);
					connection = ConnectionsManager.getInstance().createConnection(connKey, mqtt);
					synchronized (connLock) {
						ConnectionCallback callback = new ConnectionCallback(connection, connLock);
						//connection.connect(callback);
						connection.connect(options, null, callback);
						connLock.wait(TimeUnit.SECONDS.toMillis(Integer.parseInt(getConnTimeout())));
						ConnectionsManager.getInstance().setConnectionStatus(connKey, callback.isConnectionSucc());
					}
				} catch (Exception e) {
					logger.log(Priority.ERROR, e.getMessage(), e);
					ConnectionsManager.getInstance().setConnectionStatus(connKey, false);
				}
			}
		}
		
		if(payload == null) {
			if (MESSAGE_TYPE_RANDOM_STR_WITH_FIX_LEN.equals(getMessageType())) {
				payload = Util.generatePayload(Integer.parseInt(getMessageLength()));
			}
		}
		
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		
		if(!ConnectionsManager.getInstance().getConnectionStatus(connKey)) {
			result.sampleStart();
			result.setSuccessful(false);
			result.sampleEnd();
			result.setResponseMessage(MessageFormat.format("Publish failed for connection {0}.", connection));
			result.setResponseData("Publish failed becasue the connection has not been established.".getBytes());
			result.setResponseCode("500");
			return result;
		}
		try {
			byte[] toSend = new byte[]{};
			byte[] tmp = new byte[]{};

			if (MESSAGE_TYPE_HEX_STRING.equals(getMessageType())) {
				tmp = hexToBinary(getMessage());
			} else if (MESSAGE_TYPE_STRING.equals(getMessageType())) {
				tmp = getMessage().getBytes();
			} else if(MESSAGE_TYPE_RANDOM_STR_WITH_FIX_LEN.equals(getMessageType())) {
				tmp = payload.getBytes();
			}

			
			int qos = Integer.parseInt(getQOS());
			switch (qos) {
			case 0:
				qos_enum = QoS.AT_MOST_ONCE;
				break;
			case 1:
				qos_enum = QoS.AT_LEAST_ONCE;
				break;
			case 2:
				qos_enum = QoS.EXACTLY_ONCE;
				break;
			default:
				break;
			}
			
			topicName = getTopic();
			if (isAddTimestamp()) {
				byte[] timePrefix = (System.currentTimeMillis() + TIME_STAMP_SEP_FLAG).getBytes();
				toSend = new byte[timePrefix.length + tmp.length];
				System.arraycopy(timePrefix, 0, toSend, 0, timePrefix.length);
				System.arraycopy(tmp, 0, toSend, timePrefix.length , tmp.length);
			} else {
				toSend = new byte[tmp.length];
				System.arraycopy(tmp, 0, toSend, 0 , tmp.length);
			}
			result.sampleStart();
			
			final Object connLock = new Object();
			PubCallback pubCallback = new PubCallback(connLock);
			
			MqttMessage mqttMessage = new MqttMessage(toSend);
			if(qos_enum == QoS.AT_MOST_ONCE) { 
				//For QoS == 0, the callback is the same thread with sampler thread, so it cannot use the lock object wait() & notify() in else block;
				//Otherwise the sampler thread will be blocked.
				//connection.publish(topicName, toSend, qos_enum, false, pubCallback);
				connection.publish(topicName, mqttMessage, null, pubCallback);
			} else {
				synchronized (connLock) {
					connection.publish(topicName, mqttMessage, null, pubCallback);
					connLock.wait();
				}
			}
			
			result.sampleEnd();
			result.setSamplerData(new String(toSend));
			result.setSentBytes(toSend.length);
			result.setLatency(result.getEndTime() - result.getStartTime());
			result.setSuccessful(pubCallback.isSuccessful());
			
			if(pubCallback.isSuccessful()) {
				result.setResponseData("Publish successfuly.".getBytes());
				result.setResponseMessage(MessageFormat.format("publish successfully for Connection {0}.", connection));
				result.setResponseCodeOK();	
			} else {
				result.setSuccessful(false);
				result.setResponseMessage(MessageFormat.format("Publish failed for connection {0}.", connection));
				result.setResponseData("Publish failed.".getBytes());
				result.setResponseCode("500");
			}
		} catch (Exception ex) {
			logger.log(Priority.ERROR, ex.getMessage(), ex);
			result.sampleEnd();
			result.setLatency(result.getEndTime() - result.getStartTime());
			result.setSuccessful(false);
			result.setResponseMessage(MessageFormat.format("Publish failed for connection {0}.", connection));
			result.setResponseData(ex.getMessage().getBytes());
			result.setResponseCode("500");
		}
		return result;
	}

	@Override
	public void threadStarted() {
		
	}

	@Override
	public void threadFinished() {
		if (this.connection != null) {
			try {
				this.connection.disconnect(null, new IMqttActionListener() {
					
					@Override
					public void onSuccess(IMqttToken asyncActionToken) {
						logger.info(MessageFormat.format("Connection {0} disconnect successfully.", connection));
					}
					
					@Override
					public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
						logger.info(MessageFormat.format("Connection {0} failed to disconnect.", connection));
					}
				});
			} catch (MqttException e) {
				logger.error("disconnect error: " + e);
			}
		}
		if(ConnectionsManager.getInstance().containsConnection(connKey)) {
			ConnectionsManager.getInstance().removeConnection(connKey);	
		}
	}
}
