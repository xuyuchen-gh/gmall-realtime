package com.bw.gmall.realtime.common.util;

import com.bw.gmall.realtime.common.constant.Constant;

/**
 * @author xz
 * @date 2024/12/13
 */
public class SQLUtil {
    public static String getKafkaDDLSource(String groupId,String topic) {
        return "with(" +
                " 'connector' = 'kafka' ," +
                " 'properties.group.id' = ' "+ groupId+" ',  " +
                " 'topic' = '" + topic+"', " +
                " 'properties.bootstrap.servers' = ' " + Constant.KAFKA_BROKERS + "' , " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'json.ignore-parse-errors' = 'true', " + //当json解析失败的时候忽略这条数据
                " 'format' = 'json' " +
                ")";
    }
    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }
}
