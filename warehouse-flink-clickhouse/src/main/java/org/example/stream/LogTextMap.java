package org.example.stream;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LogTextMap implements FlatMapFunction<String, JSONObject> {

    @Override
    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
        try {
            System.out.println("flat: " + value);
            JSONObject parseObject = JSONObject.parseObject(value);
            if (!Optional.ofNullable(parseObject.get("filter")).isPresent() || parseObject.getBoolean("filter")) {
                return;
            }
            JSONObject commonObject = new JSONObject();
            commonObject.put("process_time", System.currentTimeMillis());
            copy(parseObject, commonObject);
            List<JSONObject> list = list(parseObject.get("data"));
            if (CollectionUtils.isNotEmpty(list)) {
                for (JSONObject object : list) {
                    JSONObject mergeObject = new JSONObject();
                    copy(commonObject, mergeObject);
                    copy(object, mergeObject);
                    System.out.println(JSONObject.toJSONString(mergeObject));
                    out.collect(mergeObject);
                }
            } else {
                System.out.println(JSONObject.toJSONString(commonObject));
                out.collect(commonObject);
            }
        } catch (Exception e) {
        }
    }

    private List<JSONObject> list(Object data) {
        List<JSONObject> list = new ArrayList<>();
        if (data instanceof JSONArray) {
            JSONArray array = (JSONArray) data;
            int size = (array).size();
            for (int i = 0; i < size; i++) {
                list.add(array.getJSONObject(i));
            }
        }
        return list;
    }

    private void copy(JSONObject source, JSONObject target) {
        copy(source, target, new ArrayList<>());
    }

    private void copy(JSONObject source, JSONObject target, List<String> filters) {
        for (String key : source.keySet()) {
            if (filters.contains(key) || source.get(key) instanceof JSONArray) {
                continue;
            }
            target.put(key, source.get(key));
        }
    }
}
