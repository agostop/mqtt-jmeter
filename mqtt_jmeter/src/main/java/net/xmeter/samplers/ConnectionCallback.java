package net.xmeter.samplers;

import java.text.MessageFormat;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.log.Priority;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;

public class ConnectionCallback implements IMqttActionListener{
	private static Logger logger = LoggingManager.getLoggerForClass();
	private Object connLock;
	private IMqttAsyncClient connection;
	private boolean connectionSucc = false;
	
	public ConnectionCallback(IMqttAsyncClient connection, Object connLock) {
		this.connection = connection;
		this.connLock = connLock;
	}
	
	@Override
	public void onSuccess(IMqttToken asyncActionToken) {
		synchronized (connLock) {
			logger.info(MessageFormat.format("The connection {0} is established successfully.", this.connection + Thread.currentThread().getName()));
			connectionSucc = true;
			this.connLock.notify();	
		}
	}
	@Override
	public void onFailure(IMqttToken asyncActionToken, Throwable value) {
		synchronized (connLock) {
			connectionSucc = false;
			logger.log(Priority.ERROR, value.getMessage(), value);
			this.connLock.notify();			
		}
	}
	/*
	@Override
	public void onSuccess(Void value) {
		synchronized (connLock) {
			logger.info(MessageFormat.format("The connection {0} is established successfully.", this.connection + Thread.currentThread().getName()));
			connectionSucc = true;
			this.connLock.notify();	
		}
	}

	@Override
	public void onFailure(Throwable value) {
		synchronized (connLock) {
			connectionSucc = false;
			logger.log(Priority.ERROR, value.getMessage(), value);
			this.connLock.notify();			
		}
	}
	*/
	
	public boolean isConnectionSucc() {
		return connectionSucc;
	}

}
