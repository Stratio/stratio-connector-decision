//package com.stratio.connector.streaming.ftest.functionalInsert;
//
//import java.util.Arrays;
//
//import com.stratio.streaming.api.IStratioStreamingAPI;
//import com.stratio.streaming.api.StratioStreamingAPIFactory;
//import com.stratio.streaming.api.messaging.ColumnNameType;
//import com.stratio.streaming.commons.constants.ColumnType;
//import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
//import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;
//import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
//import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
//import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
//import com.stratio.streaming.commons.messages.StratioStreamingMessage;
//
//import kafka.consumer.KafkaStream;
//import kafka.message.MessageAndMetadata;
//
//public class InsertLongFT {
//
//    public static void main(String[] args) throws StratioEngineConnectionException, InterruptedException,
//                    StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {
//
//        String host = "127.0.0.1";
//        final IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create()
//                        .initializeWithServerConfig(host, 9092, host, 2181);
//
//        ColumnNameType firstStreamColumn = new ColumnNameType("column1", ColumnType.LONG);
//        final String streamName = "testStream";
//
//        stratioStreamingAPI.createStream(streamName, Arrays.asList(firstStreamColumn));
//
//        KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream(streamName);
//        for (MessageAndMetadata stream : streams) {
//            StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();
//            for (ColumnNameTypeValue column : theMessage.getColumns()) {
//                System.out.println("Column: " + column.getColumn());
//                System.out.println("Value: " + column.getValue());
//                System.out.println("Type: " + column.getType());
//                System.out.println("JavaType: " + column.getValue().getClass().getName());
//                System.out.println("Cast: " + (long) (double) column.getValue());
//            }
//        }
//
//    }
//}
////
//// // @Test
//// // public void test() throws StratioEngineConnectionException, InterruptedException {
//// // String host = "127.0.0.1";
//// // final IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create()
//// // .initializeWithServerConfig(host, 9092, host, 2181);
//// //
//// // ColumnNameType firstStreamColumn = new ColumnNameType("column1", ColumnType.LONG);
//// // ColumnNameType secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING);
//// // final String streamName = "testStream" + new Random().nextLong();
//// // List columnList = Arrays.asList(firstStreamColumn, secondStreamColumn);
//// // try {
//// // stratioStreamingAPI.createStream(streamName, columnList);
//// // } catch (StratioStreamingException e) {
//// // e.printStackTrace();
//// // }
//// //
//// // String query = "from " + streamName + " select column1, column2 insert into alarms";
//// // try {
//// // String queryId = stratioStreamingAPI.addQuery(streamName, query);
//// // } catch (StratioStreamingException ssEx) {
//// // ssEx.printStackTrace();
//// // }
//// //
//// // Thread t = new Thread() {
//// // @Override
//// // public void run() {
//// // try {
//// // Thread.sleep(1000 * 10);
//// // } catch (InterruptedException e) {
//// // e.printStackTrace();
//// // }
//// // ColumnNameValue firstColumnValue = new ColumnNameValue("column1", new Long(4611686018427387905l));
//// // ColumnNameValue secondColumnValue = new ColumnNameValue("column2", "testValue");
//// // List<ColumnNameValue> streamData = Arrays.asList(firstColumnValue, secondColumnValue);
//// // try {
//// // stratioStreamingAPI.insertData(streamName, streamData);
//// // } catch (StratioStreamingException ssEx) {
//// // ssEx.printStackTrace();
//// // }
//// // };
//// // };
//// //
//// // t.start();
//// // try {
//// // KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream("alarms");
//// // for (MessageAndMetadata stream : streams) {
//// // StratioStreamingMessage theMessage = (StratioStreamingMessage) stream.message();
//// // for (ColumnNameTypeValue column : theMessage.getColumns()) {
//// // System.out.println("Column: " + column.getColumn());
//// // System.out.printf("Value: %.1f", column.getValue());
//// // System.out.println("Long Value: " + (long) (double) column.getValue());
//// // System.out.println("Type: " + column.getType());
//// // System.out.println("RealType: " + column.getValue().getClass().getName());
//// // }
//// // }
//// // } catch (StratioStreamingException ssEx) {
//// // ssEx.printStackTrace();
//// // }
//// //
//// // Thread.sleep(1000 * 30);
//// //
//// // try {
//// // stratioStreamingAPI.stopListenStream("alarms");
//// // } catch (StratioEngineStatusException | StratioAPISecurityException e) {
//// // // TODO Auto-generated catch block
//// // e.printStackTrace();
//// // }
//// //
//// // stratioStreamingAPI.close();
//// //
//// // }
////
// // }
