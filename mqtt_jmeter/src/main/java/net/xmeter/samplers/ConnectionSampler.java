package net.xmeter.samplers;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import org.apache.jmeter.JMeter;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import net.xmeter.Util;

public class ConnectionSampler extends AbstractMQTTSampler
		implements TestStateListener, ThreadListener, Interruptible, SampleListener {
	private transient static Logger logger = LoggingManager.getLoggerForClass();
//	private transient String clientId = Util.generateClientId(getConnPrefix());
	private MqttConnectOptions options;
	private boolean interrupt = false;
	private final String  TOPIC = "rongke";
	private MqttClient mqtt = null;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1859006013465470528L;

	@Override
	public boolean isKeepTimeShow() {
		return true;
	}
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		try {
			
			String clientId = getConnPrefix();
			mqtt = new MqttClient(getProtocol().toLowerCase() + "://" + getServer() + ":" + getPort(), 
					clientId, new MemoryPersistence());
			options = new MqttConnectOptions();
			options.setCleanSession(true);
			if (!"".equals(getUserNameAuth().trim())) {
				options.setUserName(getUserNameAuth());
			}
			if (!"".equals(getPasswordAuth().trim())) {
				options.setPassword(getPasswordAuth().toCharArray());
			}
			options.setConnectionTimeout(getConnTimeout());
			options.setKeepAliveInterval((short) getConnKeepAlive());
			
			options.setAutomaticReconnect(false);
			if (getConnReconnAttamptMax() > 0) 
				options.setAutomaticReconnect(true);
			
			//mqtt.setCallback(new PushCallback());
			//Topic[] topics = { new Topic("rongke" + clientId, QoS.AT_LEAST_ONCE) };
			
			options.setWill(mqtt.getTopic(TOPIC), "close".getBytes(), 0, true);
			mqtt.connect(options);
			
			int[] Qos = {0};
			String[] topic1 = {TOPIC};
			mqtt.subscribe(topic1, Qos);

			result.sampleStart();
			result.setSuccessful(true);
			result.setResponseData("Successful.".getBytes());
			result.setResponseMessage(MessageFormat.format("Connection {0} connected successfully.", mqtt));
			result.setResponseCodeOK();
		} catch (Exception e) {
			logger.log(Priority.ERROR, e.getMessage(), e);
			result.sampleEnd();
			result.setSuccessful(false);
			result.setResponseMessage(MessageFormat.format("Connection {0} connected failed.", mqtt));
			result.setResponseData("Failed.".getBytes());
			result.setResponseCode("500");
		}
		return result;
	}

	public void testEnded() {
		this.testEnded("local");
	}

	public void testEnded(String arg0) {
		this.interrupt = true;
	}

	public void testStarted() {

	}

	public void testStarted(String arg0) {
	}

	public void threadFinished() {
		if (JMeter.isNonGUI()) {
			logger.info("The work has been done, will sleep current thread for " + getConnKeepTime() + " sceconds.");
			sleepCurrentThreadAndDisconnect();
		}
	}

	private void sleepCurrentThreadAndDisconnect() {
		try {
			long start = System.currentTimeMillis();
			while ((System.currentTimeMillis() - start) <= TimeUnit.SECONDS.toMillis(getConnKeepTime())) {
				if (this.interrupt) {
					logger.info("interrupted flag is true, and stop the sleep.");
					break;
				}
				TimeUnit.SECONDS.sleep(1);
			}

			if (mqtt != null) {
				mqtt.disconnect();
				logger.log(Priority.INFO,
						MessageFormat.format("The connection {0} disconneted successfully.", mqtt));
			}
		} catch (InterruptedException e) {
			logger.log(Priority.ERROR, e.getMessage(), e);
		} catch (MqttException e) {
			logger.log(Priority.ERROR, e.getMessage(), e);
		}
	}

	public void threadStarted() {

	}

	public boolean interrupt() {
		this.interrupt = true;
		if (!JMeter.isNonGUI()) {
			logger.info("In GUI mode, received the interrupt request from user.");
		}
		return true;
	}

	/**
	 * In this listener, it can receive the interrupt event trigger by user.
	 */
	public void sampleOccurred(SampleEvent event) {
		if (!JMeter.isNonGUI()) {
			logger.info(
					"Created the sampler results, will sleep current thread for " + getConnKeepTime() + " sceconds");
			sleepCurrentThreadAndDisconnect();
		}
	}

	public void sampleStarted(SampleEvent arg0) {
	}

	public void sampleStopped(SampleEvent arg0) {
	}
}
