package hello;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;

@RestController
public class TopicManagerController {
    private static final Logger logger = LoggerFactory.getLogger(TopicManagerController.class);
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final LinkedBlockingQueue<ScheduledTopicDelete> deleteQueue = new LinkedBlockingQueue<ScheduledTopicDelete>();

    
    @RequestMapping(value = "/deletions", method = RequestMethod.GET)
    public Collection<ScheduledTopicDelete> listDeletions() {
    	return deleteQueue;
    }

    @RequestMapping("/peek")
    public ScheduledTopicDelete pop(@RequestParam(value="name", defaultValue="World") String name) {
    	return deleteQueue.peek();
    }

    @RequestMapping(value="/cluster/{cluster}", method = RequestMethod.GET)
    public String cluster(@PathVariable("cluster") String cluster) {
    	return cluster;
    }

    @RequestMapping(value="/broker/{broker}/topic/{topic}", method = RequestMethod.GET)
    public String topic(@PathVariable("broker") String broker, @PathVariable("topic") String topic) throws InterruptedException, ExecutionException {
        Properties adminClientProperties = new Properties();
        adminClientProperties.put("bootstrap.servers", broker + ":9092");
        try (AdminClient client = AdminClient.create(adminClientProperties)) {
        	DescribeTopicsResult future = client.describeTopics(Collections.singleton(topic));
        	Map<String, TopicDescription> result = future.all().get();
        	return result.get(topic).toString();
        }
    }

    @RequestMapping(value="/broker/{broker}/topic/{topic}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Queue a topic for deletion.",
    	notes = "Deletes will happen one at a time, every 10 seconds.")
    public String queueDeleteTopic(@PathVariable("broker") String broker, @PathVariable("topic") String topic) throws InterruptedException, ExecutionException {
    	deleteQueue.add(new ScheduledTopicDelete(broker, topic));
    	return "scheduled deletion for " + topic + " from broker " + broker;
    }

    @Scheduled(fixedDelay = 10000)
    public void deleteTopics() throws InterruptedException, ExecutionException {
        logger.info("Fixed Delay Task :: Execution Time - {}", dateTimeFormatter.format(LocalDateTime.now()));

        ScheduledTopicDelete scheduledDelete = deleteQueue.take();
        String topic = scheduledDelete.getTopic();
    	String broker = scheduledDelete.getBroker();
        Properties adminClientProperties = new Properties();
        adminClientProperties.put("bootstrap.servers", broker + ":9092");
        try (AdminClient client = AdminClient.create(adminClientProperties)) {
        	DeleteTopicsResult future = client.deleteTopics(Collections.singleton(topic));
        	Void result = future.all().get();
        	logger.info("deleted " + topic);
        }
    }    
    
}

