package com.maywide.bi.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapUtil {

    public static Map<String, Object> transferToLowerCase(
            Map<String, Object> orgMap) {
        Map<String, Object> resultMap = new HashMap<>();

        if (orgMap == null || orgMap.isEmpty()) {
            return resultMap;
        }

        Set<Map.Entry<String,Object>> entrySet = orgMap.entrySet();
        for (Map.Entry<String, Object> entry : entrySet) {
            String key = entry.getKey();
            Object value = entry.getValue();
            resultMap.put(key.toLowerCase(), value);
        }

        return resultMap;
    }
}
