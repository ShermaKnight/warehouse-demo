package org.example.buried;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

public class BuriedTextMap implements FlatMapFunction<String, JSONObject> {

    @Override
    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
        JSONObject parseObject = JSONObject.parseObject(value);
        if (!Optional.ofNullable(parseObject.get("division_id")).isPresent() || !Optional.ofNullable(parseObject.get("project_id")).isPresent()) {
            return;
        }
        JSONObject commonObject = new JSONObject();
        copy(parseObject, commonObject);
        List<JSONObject> list = list(parseObject.get("data"));
        if (CollectionUtils.isNotEmpty(list)) {
            for (JSONObject object : list) {
                JSONObject mergeObject = new JSONObject();
                copy(commonObject, mergeObject);
                copy(object, mergeObject);
                out.collect(mergeObject);
            }
        } else {
            out.collect(commonObject);
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
