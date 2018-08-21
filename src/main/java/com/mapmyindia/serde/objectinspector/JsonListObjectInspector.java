package com.mapmyindia.serde.objectinspector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import com.mapmyindia.serde.objectinspector.jsonparser.JsonListDeserializer;

import java.io.IOException;
import java.util.List;

/**
 * @author shubham
 */

public class JsonListObjectInspector extends StandardListObjectInspector {

    public JsonListObjectInspector(ObjectInspector listElementObjectInspector) {

        super(listElementObjectInspector) ;
    }

    @Override
    public List<?> getList(Object data) {
        return getListFromObjectMapper(data) ;
    }

    @Override
    public Object getListElement(Object data, int index) {
        return getListFromObjectMapper(data).get(index) ;
    }

    @Override
    public int getListLength(Object data) {
        List<?> list = getListFromObjectMapper(data) ;
        if (list == null) return -1 ;
        return list.size() ;
    }

    private List<?> getListFromObjectMapper(Object data) {
        if (JsonObjectInspectorUtils.checkObject(data) == null) return null ;

        ObjectMapper customListObjectMapper = new ObjectMapper() ;
        SimpleModule module = new SimpleModule("JsonListDeserializer") ;
        module.addDeserializer(Object.class, new JsonListDeserializer(Object.class)) ;
        customListObjectMapper.registerModule(module) ;
        try {
            List<Object> jsonList = customListObjectMapper.readValue(data.toString(), new TypeReference<Object>(){}) ;
            return jsonList ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null ;
    }
}