package com.nuomuo.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nuomuo.GmallConstants;
import com.nuomuo.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 将 MySQL 变化的数据 转移到 kafka
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {

        // 从 canal 获取数据
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("node1", 11111), "example", "", "");
        while (true){

            canalConnector.connect();
            canalConnector.subscribe("gmall2021.*");
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() <= 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();
                    //Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断 entryType 是否为 ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        /*
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            String value = rowData.getAfterColumns(1).getValue();
                            System.out.println(value);

                        }
                         */
                        //TODO 根据条件获取数据
                        handler(tableName, eventType, rowDatasList);

                    }
                }
            }
        }
        // 将数据发往 kafka
    }
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        // 获取订单表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toString());
            }
        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            //获取用户表的新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //获取存放列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //获取每个列
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }


}
