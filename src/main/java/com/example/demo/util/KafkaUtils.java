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

    public static void writeToKafka(String topic, String msg) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);

        KafkaProducer producer = new KafkaProducer<String, String>(props);
        try {
            producer.send( new ProducerRecord<>(topic, msg)).get();
            System.out.println("发送数据 "+msg);
            //producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
       KafkaUtils.writeToKafka(topic,"{\"id\":\"61966493\"}");
//,"skuname":"Febreze 芬亦飘 车用空气清新剂甜蜜果香2mL + Febreze 芬亦飘 车用空气清新剂户外清芬2mL","price":"69","sales":"2","comment":"2","img":"http://d6.yihaodianimg.com/N09/M04/DE/CF/ChEi2FdiqH-AfROmAAIjKqmP6_409800_230*230.jpg","detailurl":"http://item.yhd.com/item/61966493"
       //KafkaUtils.writeToKafka("{skuid=61966493,skuname=Febreze 芬亦飘 车用空气清新剂甜蜜果香2mL + Febreze 芬亦飘 车用空气清新剂户外清芬2mL,price=69,sales=2,comment:2,img=http://d6.yihaodianimg.com/N09/M04/DE/CF/ChEi2FdiqH-AfROmAAIjKqmP6_409800_230*230.jpg,detailurl=http://item.yhd.com/item/61966493}");
    }


}
