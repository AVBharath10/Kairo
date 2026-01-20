import java.util.*;
import java.util.ArrayList;
import java.util.List;

//Event class
class Event {

    private final String eventType;
    private final String payLoad;
    private final long ts;

    public Event(String et, String pl) {
        this.eventType = et;
        this.payLoad = pl;
        this.ts = System.currentTimeMillis();
    }

    public String getEvent() {
        return eventType;
    }

    public String getpayload() {
        return payLoad;
    }

    public long getTimeStamp() {
        return ts;
    }
}

//Eventlog class
public class EventLog {

    private final List<Event> events = new ArrayList<>();

    public synchronized long append(Event event) {
        events.add(event);
        return events.size() - 1;
    }

    public synchronized List<Event> readFrom(long offset) {
        return events.subList((int) offset, events.size());
    }
}

//Topic Class
public class Topic {

    private List<EventLog> partitions;

    public Topic(int count) {
        this.partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new EventLog());
        }
    }

    private int choosePartition(String key) {
        return Math.abs(key.hashCode() % partitions.size());
    }

    public long append(String key, Event event) {
        int p = choosePartition(key);
        return partitions.get(p).append(event);
    }

    public EventLog getPartition(int partition) {
        return partitions.get(partition);
    }
}

//Broker Class
public class Broker {

    private final Map<String, Topic> map = new HashMap<>();

    public void newtopic(String name, int partitions) {
        map.put(name, new Topic(partitions));
    }

    public void produce(String topic, String key, Event event) {
        map.get(topic).append(key, event);
    }

    public Topic getTopic(String topic) {
        return map.get(topic);
    }
}

//Consumer class
class Consumer {

    private final Map<String, Long> map = new HashMap<>();

    public List<Event> poll(Broker broker, String topic, int partition) {
        String key = topic + "-" + partition;
        long offsets = map.getOrDefault(key, 0L);
        EventLog log = broker.getTopic(topic).getPartition(partition);
        List<Event> events = log.readFrom(offsets);

        map.put(key, offsets + events.size());
        return events;
    }
}

public class Main {

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.newtopic("orders", 3);

        broker.produce(
            "orders",
            "order1",
            new Event("OrderPlaced", "{amount:500}")
        );

        Consumer consumer = new Consumer();
        consumer
            .poll(broker, "orders", 0)
            .forEach(e -> System.out.println(e.getEvent()));
    }
}
