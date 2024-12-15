package com.bw.gmall.realtime.common.function;

/**
 * @author xz
 * @date 2024/12/15
 */
import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}