package net.xmeter.samplers;

import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;

public class ConnectionsManager {
	private ConcurrentHashMap<String, IMqttAsyncClient> connections = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Boolean> connectionsStatus = new ConcurrentHashMap<>();
	
	private static ConnectionsManager connectionsManager = new ConnectionsManager();
	private ConnectionsManager() {
		
	}
	
	public static synchronized ConnectionsManager getInstance() {
		return connectionsManager;
	}
	
	public IMqttAsyncClient createConnection(String key, IMqttAsyncClient mqtt) {
		//CallbackConnection conn = mqtt.callbackConnection();
		connections.put(key, mqtt);
		return mqtt;
	}
	
	public IMqttAsyncClient getConnection(String key) {
		return this.connections.get(key);
	}
	
	public boolean containsConnection(String key) {
		return connections.containsKey(key);
	}
	
	public void removeConnection(String key) {
		this.connections.remove(key);
	}
	
	public void setConnectionStatus(String key, Boolean status) {
		connectionsStatus.put(key, status);
	}
	
	public boolean getConnectionStatus(String key) {
		if(!connectionsStatus.containsKey(key)) {
			throw new RuntimeException("Cannot find conn status for key: " + key);
		}
		return connectionsStatus.get(key);
	}
}
