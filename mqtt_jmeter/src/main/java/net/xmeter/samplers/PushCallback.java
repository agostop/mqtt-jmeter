package net.xmeter.samplers;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class PushCallback implements MqttCallback{

	public PushCallback() {
	}

	public void connectionLost(Throwable cause) {
		System.out.println("连接断开");
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		System.out.println(token.isComplete());
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("接收消息主题: " + topic);
		System.out.println("接收消息Qos: " + message.getQos());
		System.out.println("接收消息内容: " + new String(message.getPayload()));
	}

}
