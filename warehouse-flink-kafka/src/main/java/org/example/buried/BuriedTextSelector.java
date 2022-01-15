package org.example.buried;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BuriedTextSelector implements KeySelector<JSONObject, String> {

    @Override
    public String getKey(JSONObject value) throws Exception {
        return Stream.of(
                (String) value.get("division_id"),
                (String) value.get("project_id")
        ).collect(Collectors.joining("_"));
    }
}
