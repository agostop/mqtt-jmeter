package net.xmeter.samplers;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import net.xmeter.SubBean;
import net.xmeter.Util;

@SuppressWarnings("deprecation")
public class SubSampler extends AbstractMQTTSampler implements ThreadListener {
	//private transient MQTT mqtt = new MQTT();
	//private transient MqttAsyncClient mqtt = null;
	private transient MqttAsyncClient connection = null;
	private transient static Logger logger = LoggingManager.getLoggerForClass();

	private boolean connectFailed = false;
	private boolean subFailed = false;
	private boolean receivedMsgFailed = false;
	private int receivedTimeOut = 0;

	private transient ConcurrentLinkedQueue<SubBean> batches = new ConcurrentLinkedQueue<>();
	private boolean printFlag = false;

	private transient Object dataLock = new Object();
	
	private int qos = QOS_0;
	private String connKey = "";
	/**
	 * 
	 */
	private static final long serialVersionUID = 2979978053740194951L;

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
	
	public String getSampleCondition() {
		return getPropertyAsString(SAMPLE_CONDITION, SAMPLE_ON_CONDITION_OPTION1);
	}
	
	public void setSampleCondition(String option) {
		setProperty(SAMPLE_CONDITION, option);
	}
	
	public Integer getSampleCount() {
		return getPropertyAsInt(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_COUNT);
	}
	
	public void setSampleCount(String count) {
		try {
			int temp = Integer.parseInt(count);
			if(temp < 1) {
				logger.info("Invalid sample message count value.");
				throw new IllegalArgumentException();
			}
			setProperty(SAMPLE_CONDITION_VALUE, count);
		} catch(Exception ex) {
			logger.info("Invalid count value, set to default value.");
			setProperty(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_COUNT);
		}
	}
	
	public String getSampleElapsedTime() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_ELAPSED_TIME_SEC);
	}
	
	public void setSampleElapsedTime(String elapsedTime) {
		try {
			int temp = Integer.parseInt(elapsedTime);
			if(temp <= 0) {
				throw new IllegalArgumentException();
			}
			setProperty(SAMPLE_CONDITION_VALUE, elapsedTime);
		}catch(Exception ex) {
			logger.info("Invalid elapsed time value, set to default value: " + elapsedTime);
			setProperty(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_ELAPSED_TIME_SEC);
		}
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
	
	public void setRecvTimeOut(String timeOut) {
		try {
			int temp = Integer.parseInt(timeOut);
			if(temp <= 0) {
				throw new IllegalArgumentException();
			}
			setProperty(RECV_TIMEOUT, timeOut);
		}catch(Exception ex) {
			logger.info("Invalid recv time value, set to default value: " + DEFAULT_RECV_TIMEOUT);
			setProperty(RECV_TIMEOUT, DEFAULT_RECV_TIMEOUT);
		}
	}
	
	public Integer getRecvTimeOut() {
		return getPropertyAsInt(RECV_TIMEOUT, DEFAULT_RECV_TIMEOUT);
	}

	public String getConnClientId() {
		return getPropertyAsString(CONN_CLIENT_ID_PREFIX, DEFAULT_CONN_PREFIX_FOR_SUB);
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
		final boolean sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());
		final int sampleCount = getSampleCount();
		connKey = getKey();
		if(connection == null) {
			connection = (MqttAsyncClient) ConnectionsManager.getInstance().getConnection(connKey);
			final String topicName= getTopic();
			if(connection != null) {
				logger.info("Use the shared connection: " + connection);
				setListener(sampleByTime, sampleCount);
				listenToTopics(topicName);
			} else {
				 // first loop, initializing ..
				try {
					//mqtt.setVersion(getMqttVersion());
					//mqtt.setKeepAlive((short) Integer.parseInt(getConnKeepAlive()));
					MqttConnectOptions options = new MqttConnectOptions();
					options.setKeepAliveInterval(Integer.parseInt(getConnKeepAlive()));
					options.setAutomaticReconnect(Integer.parseInt(getConnAttamptMax()) > 0);
					options.setMqttVersion(getMqttVersion());
					options.setConnectionTimeout(Integer.parseInt(getConnTimeout()));
		
					String clientId = null;
					receivedTimeOut = getRecvTimeOut();
					if(isClientIdSuffix()) {
						clientId = Util.generateClientId(getConnClientId());
					} else {
						clientId = getConnClientId();
					}
					
					if (!"".equals(getUserNameAuth().trim())) {
						options.setUserName(getUserNameAuth());
					}
					if (!"".equals(getPasswordAuth().trim())) {
						options.setPassword(getPasswordAuth().toCharArray());
					}
					
					MqttAsyncClient mqtt = new MqttAsyncClient(getProtocol().toLowerCase() + "://" + getServer() + ":" + getPort(), clientId, null);
					
					connection = (MqttAsyncClient) ConnectionsManager.getInstance().createConnection(connKey, mqtt);
					setListener(sampleByTime, sampleCount);
					connection.connect(options, null, new IMqttActionListener() {

						@Override
						public void onSuccess(IMqttToken asyncActionToken) {
							listenToTopics(topicName);
							ConnectionsManager.getInstance().setConnectionStatus(connKey, true);
							
						}

						@Override
						public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
							connectFailed = true;
							ConnectionsManager.getInstance().setConnectionStatus(connKey, false);
						}
						
					});
					
				} catch (Exception e) {
					logger.log(Priority.ERROR, e.getMessage(), e);
				}
			
			}
		}
		
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		
		if (connectFailed) {
			result.sampleStart();
			return fillFailedResult(sampleByTime, result, "Connection failed.");
		} else if (subFailed) {
			result.sampleStart();
			return fillFailedResult(sampleByTime, result, "Failed to subscribe to topic.");
		} else if (receivedMsgFailed) {
			result.sampleStart();
			return fillFailedResult(sampleByTime, result, "Failed to receive message.");
		}
		
		if(sampleByTime) {
			try {
				TimeUnit.MILLISECONDS.sleep(Long.parseLong(getSampleElapsedTime()));
			} catch (InterruptedException e) {
				logger.info("Received exception when waiting for notification signal: " + e.getMessage());
			}
			synchronized (dataLock) {
				result.sampleStart();
				return produceResult(result, sampleByTime);	
			}
		} else {
			synchronized (dataLock) {
				int receivedCount1 = (batches.isEmpty() ? 0 : batches.element().getReceivedCount());
				boolean needWait = false;
				if(receivedCount1 < sampleCount) {
					needWait = true;
				}
				
				//logger.info(System.currentTimeMillis() + ": need wait? receivedCount=" + receivedCount + ", sampleCount=" + sampleCount);
				if(needWait) {
					try {
						dataLock.wait(receivedTimeOut);
					} catch (InterruptedException e) {
						logger.info("Received exception when waiting for notification signal: " + e.getMessage());
					}
				}
				result.sampleStart();
				return produceResult(result, sampleByTime);
			}
		}
	}
	
	private SampleResult produceResult(SampleResult result, boolean sampleByTime) {
		SubBean bean = batches.poll();
		if(bean == null) { //In case selected with time interval
			bean = new SubBean();
		}
		int receivedCount = bean.getReceivedCount();
		List<String> contents = bean.getContents();
		String message = MessageFormat.format("Received {0} of message\n.", receivedCount);
		StringBuilder content = new StringBuilder("");
		if (isDebugResponse()) {
			for (int i = 0; i < contents.size(); i++) {
				content.append(contents.get(i));
				content.append(" \n");
			}
		}
		if (bean.getReceivedCount() == getSampleCount()) {
			result = fillOKResult(result, bean.getReceivedMessageSize(), message, content.toString());
		} else {
			return fillFailedResult(sampleByTime, result, message);
		}
		
		if(receivedCount == 0) {
			result.setEndTime(result.getStartTime());
		} else {
			if (isAddTimestamp()) {
				result.setEndTime(result.getStartTime() + (long) bean.getAvgElapsedTime());
				result.setLatency((long) bean.getAvgElapsedTime());
			} else {
				result.setEndTime(result.getStartTime());	
			}
		}
		result.setSampleCount(receivedCount);

		return result;
	}
	
	private void listenToTopics(final String topicName) {
		try {
			qos = Integer.parseInt(getQOS());
		} catch(Exception ex) {
			logger.error(MessageFormat.format("Specified invalid QoS value {0}, set to default QoS value {1}!", ex.getMessage(), qos));
			qos = QOS_0;
		}
		
		final String[] paraTopics = topicName.split(",");
		try {
			connection.subscribe(paraTopics, new int[] { qos }, null, new IMqttActionListener() {

				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("sub successful, topic length is " + paraTopics.length);

				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					subFailed = true;
					//connection.kill(null);
				}
			});
		} catch (MqttException e) {
			logger.error("subscribe error: " + e);
		}
		/*
		Topic[] topics = new Topic[paraTopics.length];
		if(qos < 0 || qos > 2) {
			logger.error("Specified invalid QoS value, set to default QoS value " + qos);
			qos = QOS_0;
		}
		for(int i = 0; i < topics.length; i++) {
			if (qos == QOS_0) {
				topics[i] = new Topic(paraTopics[i], QoS.AT_MOST_ONCE);
			} else if (qos == QOS_1) {
				topics[i] = new Topic(paraTopics[i], QoS.AT_LEAST_ONCE);
			} else {
				topics[i] = new Topic(paraTopics[i], QoS.EXACTLY_ONCE);
			}
		}
		*/

	}
	
	private void setListener(final boolean sampleByTime, final int sampleCount) {
		
		connection.setCallback(new MqttCallback() {
			
			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				if(sampleByTime) {
					synchronized (dataLock) {
						String msg = new String(message.getPayload());
						logger.debug(MessageFormat.format("recevice msg: {0}", msg ));
						handleSubBean(sampleByTime, msg, sampleCount);
					}
				} else {
					synchronized (dataLock) {
						SubBean bean = handleSubBean(sampleByTime, new String(message.getPayload()), sampleCount);
						//logger.info(System.currentTimeMillis() + ": need notify? receivedCount=" + bean.getReceivedCount() + ", sampleCount=" + sampleCount);
						if(bean.getReceivedCount() == sampleCount) {
							dataLock.notify();
						}
					}
				}
			}
			
			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				
			}
			
			@Override
			public void connectionLost(Throwable cause) {
				connectFailed = true;
				//connection.kill(null);
			}
		});
		
		/*
		connection.listener(new Listener() {
			@Override
			public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
				try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					body.writeTo(baos);
					String msg = baos.toString();
					ack.run();
					
					if(sampleByTime) {
						synchronized (dataLock) {
							handleSubBean(sampleByTime, msg, sampleCount);
						}
					} else {
						synchronized (dataLock) {
							SubBean bean = handleSubBean(sampleByTime, msg, sampleCount);
							//logger.info(System.currentTimeMillis() + ": need notify? receivedCount=" + bean.getReceivedCount() + ", sampleCount=" + sampleCount);
							if(bean.getReceivedCount() == sampleCount) {
								dataLock.notify();
							}
						}
					}
				} catch (IOException e) {
					logger.log(Priority.ERROR, e.getMessage(), e);
				}
			}

			@Override
			public void onFailure(Throwable value) {
				connectFailed = true;
				connection.kill(null);
			}

			@Override
			public void onDisconnected() {
			}

			@Override
			public void onConnected() {
			}
		});
		*/
	}
	
	private SubBean handleSubBean(boolean sampleByTime, String msg, int sampleCount) {
		SubBean bean = null;
		if(batches.isEmpty()) {
			bean = new SubBean();
			batches.add(bean);
		} else {
			SubBean[] beans = new SubBean[batches.size()];
			batches.toArray(beans);
			bean = beans[beans.length - 1];
		}
		
		if((!sampleByTime) && (bean.getReceivedCount() == sampleCount)) { //Create a new batch when latest bean is full.
			logger.info("The tail bean is full, will create a new bean for it.");
			bean = new SubBean();
			batches.add(bean);
		}
		if (isAddTimestamp()) {
			long now = System.currentTimeMillis();
			//int index = msg.indexOf(TIME_STAMP_SEP_FLAG);
			
			try {
				JSONObject recvMsg = JSONObject.parseObject(msg).getJSONObject("result").getJSONArray("msg").getJSONObject(0);
				if (recvMsg == null || (!recvMsg.containsKey("time") && (!printFlag))) {
					logger.info("Payload does not include timestamp: " + msg);
					printFlag = true;
				} else if (recvMsg != null && recvMsg.containsKey("time")) {
					Long start = recvMsg.getLong("time");
					Long elapsed = now - start * 1000;
					logger.info(MessageFormat.format("Payload get start time : {0}, now time : {1}", start, now));
					printFlag = true;
					double avgElapsedTime = bean.getAvgElapsedTime();
					int receivedCount = bean.getReceivedCount();
					avgElapsedTime = (avgElapsedTime * receivedCount + elapsed) / (receivedCount + 1);
					bean.setAvgElapsedTime(avgElapsedTime);
				}
			} catch (JSONException e) {
				logger.error("msg parse failed. errormsg: " + e);
			}

		}
		if (isDebugResponse()) {
			bean.getContents().add(msg);
		}
		bean.setReceivedMessageSize(bean.getReceivedMessageSize() + msg.length());
		bean.setReceivedCount(bean.getReceivedCount() + 1);
		return bean;
	}

	private SampleResult fillFailedResult(boolean sampleByTime, SampleResult result, String message) {
		result.setResponseCode("500");
		result.setSuccessful(false);
		result.setResponseMessage(message);
		result.setResponseData(message.getBytes());
		result.setEndTime(result.getStartTime());
		
		if(sampleByTime) {
			try {
				TimeUnit.MILLISECONDS.sleep(Long.parseLong(getSampleElapsedTime()));
			} catch (InterruptedException e) {
				logger.info("Received exception when waiting for notification signal: " + e.getMessage());
			}
		}
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

	@Override
	public void threadStarted() {
		//logger.info("*** in threadStarted");
		boolean sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());
		if(!sampleByTime) {
			logger.info("Configured with sampled on message count, will not check message sent time.");
			return;
		}
	}
	
	@Override
	public void threadFinished() {
		//logger.info(System.currentTimeMillis() + ", threadFinished");
		//logger.info("*** in threadFinished");
		try {
			this.connection.disconnect(null, new IMqttActionListener() {
				
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info(MessageFormat.format("Connection {0} disconnect successfully.", connection.getClientId()));
				}
				
				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.info(MessageFormat.format("Connection {0} failed to disconnect.", connection.getClientId()));
				}
			});
		} catch (MqttException e) {
			logger.error("disconnect error: " + e);
		}
		
		/*
		this.connection.disconnect(new Callback<Void>() {
			@Override
			public void onSuccess(Void value) {
				logger.info(MessageFormat.format("Connection {0} disconnect successfully.", connection));
			}

			@Override
			public void onFailure(Throwable value) {
				logger.info(MessageFormat.format("Connection {0} failed to disconnect.", connection));
			}
		});
		*/
		
		if(ConnectionsManager.getInstance().containsConnection(connKey)) {
			ConnectionsManager.getInstance().removeConnection(connKey);	
		}
	}
}
