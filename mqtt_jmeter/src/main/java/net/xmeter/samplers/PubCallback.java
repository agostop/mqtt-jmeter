package net.xmeter.samplers;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;

public class PubCallback implements IMqttActionListener{
	private static Logger logger = LoggingManager.getLoggerForClass();
	private boolean successful = false;
	private Object connLock;
	
	public PubCallback(Object connLock) {
		this.connLock = connLock;
	}
	
	@Override
	public void onSuccess(IMqttToken asyncActionToken) {
		//If QoS == 0, then the current thread is the same thread of caller thread.
		//Else if QoS == 1 | 2, then the current thread is hawtdispatch-DEFAULT-x
		synchronized (connLock) {
			this.successful = true;
			connLock.notify();
		}	
	}

	@Override
	public void onFailure(IMqttToken asyncActionToken, Throwable value) {
		synchronized (connLock) {
			this.successful = false;
			logger.log(Priority.ERROR, value.getMessage(), value);
			connLock.notify();
		}	
	}
	
	public boolean isSuccessful() {
		return successful;
	}

}
