package net.xmeter.samplers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import net.xmeter.Util;

@SuppressWarnings("deprecation")
public class SubSamplerWSS extends AbstractMQTTSampler implements ThreadListener {
	private transient MQTT mqtt = new MQTT();
	private transient CallbackConnection connection = null;
	private transient static Logger logger = LoggingManager.getLoggerForClass();

	private boolean connectFailed = false;
	private boolean subFailed = false;
	private boolean receivedMsgFailed = false;

	private int receivedMessageSize = 0;
	private int receivedCount = 0;
	private double avgElapsedTime = 0f;

	private List<String> contents = new ArrayList<String>();
	private boolean printFlag = false;

	private Object lock = new Object();

	private int qos = QOS_0;
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

		long startTime = System.currentTimeMillis();
		result.sampleStart();

		if (connectFailed) {
			return fillFailedResult(result, MessageFormat.format("Connection {0} connected failed.", connection));
		} else if (subFailed) {
			return fillFailedResult(result, "Failed to subscribe to topic.");
		} else if (receivedMsgFailed) {
			return fillFailedResult(result, "Failed to receive message.");
		}

		synchronized (lock) {
			String message = MessageFormat.format("Received {0} of message\n.", receivedCount);
			// getLogger().info(message);
			StringBuffer content = new StringBuffer("");
			if (isDebugResponse()) {
				for (int i = 0; i < contents.size(); i++) {
					content.append(contents.get(i) + " \n");
				}
			}
			// System.out.println(MessageFormat.format("receivedMessageSize {0}
			// with receivedCount {1}.", receivedMessageSize, receivedCount));
			int avgSize = 0;
			if (receivedCount != 0) {
				avgSize = receivedMessageSize / receivedCount;
			}
			result = fillOKResult(result, avgSize, message, content.toString());
			if (isAddTimestamp()) {
				result.setEndTime(startTime + (long) this.avgElapsedTime);
			}
			result.setSampleCount(receivedCount);

			receivedMessageSize = 0;
			receivedCount = 0;
			avgElapsedTime = 0f;
			contents.clear();

			return result;
		}
	}

	private SampleResult fillFailedResult(SampleResult result, String message) {
		result.setResponseCode("500");
		result.setSuccessful(false);
		result.setResponseMessage(message);
		result.setResponseData("Failed.".getBytes());
		result.sampleEnd();
		return result;
	}

	private SampleResult fillOKResult(SampleResult result, int size, String message, String contents) {
		result.setResponseCode("200");
		result.setSuccessful(true);
		result.setResponseMessage(message);
		result.setBodySize(size);
		result.setBytes(size);
		result.setResponseData(contents.getBytes());
		result.sampleEnd();
		return result;
	}

	public void threadFinished() {
		this.connection.disconnect(new Callback<Void>() {
			public void onSuccess(Void value) {
				logger.info(MessageFormat.format("Connection {0} disconnect successfully.", connection));
			}

			public void onFailure(Throwable value) {
				logger.info(MessageFormat.format("Connection {0} failed to disconnect.", connection));
			}
		});
	}

	public void threadStarted() {
		try {
			mqtt.setHost(getProtocol().toLowerCase() + "://" + getServer() + ":" + getPort());
			mqtt.setKeepAlive((short) getConnKeepAlive());

			String clientId = Util.generateClientId(getConnPrefix());
			mqtt.setClientId(clientId);

			mqtt.setConnectAttemptsMax(getConnAttamptMax());
			mqtt.setReconnectAttemptsMax(getConnReconnAttamptMax());

			if (!"".equals(getUserNameAuth().trim())) {
				mqtt.setUserName(getUserNameAuth());
			}
			if (!"".equals(getPasswordAuth().trim())) {
				mqtt.setPassword(getPasswordAuth());
			}

			connection = mqtt.callbackConnection();
			connection.listener(new Listener() {
				public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
					try {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						body.writeTo(baos);
						String msg = baos.toString();
						ack.run();
						synchronized (lock) {
							if (isAddTimestamp()) {
								long now = System.currentTimeMillis();
								int index = msg.indexOf(TIME_STAMP_SEP_FLAG);
								if (index == -1 && (!printFlag)) {
									logger.info("Payload does not include timestamp: " + msg);
									printFlag = true;
								} else if (index != -1) {
									long start = Long.parseLong(msg.substring(0, index));
									long elapsed = now - start;
									avgElapsedTime = (avgElapsedTime * receivedCount + elapsed) / (receivedCount + 1);
								}
							}
							if (isDebugResponse()) {
								contents.add(msg);
							}
							receivedMessageSize += msg.length();
							receivedCount++;
						}
					} catch (IOException e) {
						logger.log(Priority.ERROR, e.getMessage(), e);
					}
				}

				public void onFailure(Throwable value) {
					connectFailed = true;
					connection.kill(null);
				}

				public void onDisconnected() {
				}

				public void onConnected() {
				}
			});

			connection.connect(new Callback<Void>() {
				public void onSuccess(Void value) {
					String topicName = getTopic();
					Topic[] topics = new Topic[1];
					if (qos == QOS_0) {
						topics[0] = new Topic(topicName, QoS.AT_MOST_ONCE);
					} else if (qos == QOS_1) {
						topics[0] = new Topic(topicName, QoS.AT_LEAST_ONCE);
					} else {
						topics[0] = new Topic(topicName, QoS.EXACTLY_ONCE);
					}

					connection.subscribe(topics, new Callback<byte[]>() {
						public void onSuccess(byte[] value) {
							logger.info("sub successful: " + new String(value));
						}

						public void onFailure(Throwable value) {
							subFailed = true;
							connection.kill(null);
						}
					});
				}

				public void onFailure(Throwable value) {
					connectFailed = true;
				}
			});
		} catch (Exception e) {
			logger.log(Priority.ERROR, e.getMessage(), e);
		}

	}

}
