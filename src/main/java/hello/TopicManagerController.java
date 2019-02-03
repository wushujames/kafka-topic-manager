package hello;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TopicManagerController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private final ConcurrentLinkedQueue<String> deleteQueue = new ConcurrentLinkedQueue<String>(); 
    
    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    }

    @RequestMapping("/deletions")
    public ConcurrentLinkedQueue<String> deletion(@RequestParam(value="name", defaultValue="World") String name) {
    	deleteQueue.add("" + counter.incrementAndGet() + " " + name);
    	return deleteQueue;
    }

    @RequestMapping("/pop")
    public String pop(@RequestParam(value="name", defaultValue="World") String name) {
    	return deleteQueue.poll();
    }

    @RequestMapping(value="/cluster/{cluster}", method = RequestMethod.GET)
    public String cluster(@PathVariable("cluster") String cluster) {
    	return cluster;
    }

    @RequestMapping(value="/broker/{broker}/topic/{topic}", method = RequestMethod.GET)
    public String topic(@PathVariable("broker") String broker, @PathVariable("topic") String topic) throws InterruptedException, ExecutionException {
        Properties adminClientProperties = new Properties();
        adminClientProperties.put("bootstrap.servers", "localhost:9092");
        try (AdminClient client = AdminClient.create(adminClientProperties)) {
        	DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        	return result.all().get().toString();
        }
    	
    }

}

