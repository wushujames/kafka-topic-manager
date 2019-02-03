package hello;

public class ScheduledTopicDelete {

	private final String broker;
	private final String topic;
	
	public ScheduledTopicDelete(String broker, String topic) {
		this.broker = broker;
		this.topic = topic;
	}

	public String getBroker() {
		return broker;
	}

	public String getTopic() {
		return topic;
	}

	@Override
	public String toString() {
		return "ScheduledTopicDelete [broker=" + broker + ", topic=" + topic + "]";
	}
}
