package com.mapmyindia.serde.objectinspector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector ;

import java.io.IOException;
import java.util.Map;

/**
 * @author shubham
 */

public class JsonMapObjectInspector extends StandardMapObjectInspector {

    public JsonMapObjectInspector(ObjectInspector mapKeyObjectInspector,
                                  ObjectInspector mapValueObjectInspector) {
        super(mapKeyObjectInspector, mapValueObjectInspector) ;
    }

    @Override
    public Map<?, ?> getMap(Object data) {
        return getMapFromObjectMapper(data) ;
    }

    @Override
    public int getMapSize(Object data) {
        Map<?, ?> jsonMap = getMapFromObjectMapper(data) ;
        if (jsonMap == null) return -1 ;
        return jsonMap.size() ;
    }

    @Override
    public Object getMapValueElement(Object data, Object key) {
        if (JsonObjectInspectorUtils.checkObject(data) == null) return null ;
        Map<?, ?> jsonMap = getMapFromObjectMapper(data) ;
        if (jsonMap.containsKey(key)) {
            return jsonMap.get(key) ;
        }
        return null ;
    }

    private Map<?, ?> getMapFromObjectMapper(Object data) {
        if (JsonObjectInspectorUtils.checkObject(data) == null) return null ;
        ObjectMapper objectMapper = new ObjectMapper() ;
        try {
            Map<Object, Object>  jsonMap = objectMapper.readValue((String)data, new TypeReference<Object>(){});
            return jsonMap ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null ;
    }
}
