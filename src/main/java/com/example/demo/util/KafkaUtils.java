package com.example.demo.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by zh on 2017/11/14.
 */
public class KafkaUtils {

    private static final String broker_list = "yksp020007.youkeshu.com:6667";
    private static final String topic = "sku";

    private static final String keySerializer = "org.apache.kafka.common.serialization.IntegerSerializer";
    private static final String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     *
     props.put("bootstrap.servers", "localhost:9092");

     是判别请求是否为完整的条件（就是是判断是不是成功发送了）。我们指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的。
     props.put("acks", "all");
     如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。
     props.put("retries", 0);
     props.put("batch.size", 16384);
     props.put("linger.ms", 1);
     控制生产者可用的缓存总量，如果消息发送速度比其传输到服务器的快，将会耗尽这个缓存空间。当缓存空间耗尽，其他发送调用将被阻塞，阻塞时间的阈值通过max.block.ms设定，之后它将抛出一个TimeoutException
     props.put("buffer.memory", 33554432);
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

     设置enable.auto.commit,偏移量由auto.commit.interval.ms控制自动提交的频率
     props.put("enable.auto.commit", "true");
     props.put("auto.commit.interval.ms", "1000");
     */

    public static void writeToKafka(String topic, String msg) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);

        KafkaProducer producer = new KafkaProducer<String, String>(props);
        try {
            /**
             send方法是异步的，添加消息到缓冲区等待发送，并立即返回。生产者将单个的消息批量在一起发送来提高效率
             */
            producer.send( new ProducerRecord<>(topic, msg)).get();
            System.out.println("发送数据 "+msg);
            //producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    public static void main(String[] args) {
       KafkaUtils.writeToKafka(topic,"{\"id\":\"61966493\"}");
//,"skuname":"Febreze 芬亦飘 车用空气清新剂甜蜜果香2mL + Febreze 芬亦飘 车用空气清新剂户外清芬2mL","price":"69","sales":"2","comment":"2","img":"http://d6.yihaodianimg.com/N09/M04/DE/CF/ChEi2FdiqH-AfROmAAIjKqmP6_409800_230*230.jpg","detailurl":"http://item.yhd.com/item/61966493"
       //KafkaUtils.writeToKafka("{skuid=61966493,skuname=Febreze 芬亦飘 车用空气清新剂甜蜜果香2mL + Febreze 芬亦飘 车用空气清新剂户外清芬2mL,price=69,sales=2,comment:2,img=http://d6.yihaodianimg.com/N09/M04/DE/CF/ChEi2FdiqH-AfROmAAIjKqmP6_409800_230*230.jpg,detailurl=http://item.yhd.com/item/61966493}");
    }


}
