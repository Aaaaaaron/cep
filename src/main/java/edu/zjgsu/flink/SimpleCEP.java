//package edu.zjgsu.flink;
//
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.hadoop.yarn.event.Event;
//
///**
// * Created by AH on 2016/12/30.
// */
//public class SimpleCEP {
//    public static void main ( String[] args ) {
//        DataStream<Event >;
//        Pattern<Event, ?> pattern = Pattern.begin("start").where(evt -> evt.getId() == 42)
//                .next("middle").subtype(SubEvent.class).where(subEvt -> subEvt.getVolume() >= 10.0)
//                .followedBy("end").where(evt -> evt.getName().equals("end"));
//    }
//}
