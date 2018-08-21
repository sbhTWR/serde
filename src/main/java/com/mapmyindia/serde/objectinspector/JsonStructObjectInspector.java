package com.mapmyindia.serde.objectinspector;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector ;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;


import com.mapmyindia.serde.objectinspector.jsonparser.JsonStructDeserializer ;

/**
 * @author sbhTWR
 */

public class JsonStructObjectInspector extends StandardStructObjectInspector {

    /*
     * Cache which caches map data for a single row. Reduces computation when multiple rows are selected.
     */

    HashMap<String, Object> map; ;

    public JsonStructObjectInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors) ;
        map = new  HashMap<>() ;
    }

    /*
     * We recieve a List<Object> as data, so we need to get() it.
     */

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (JsonObjectInspectorUtils.checkObject(data) == null) return null ;
        MyField f = (MyField)fieldRef ;
            ObjectMapper customStructObjectMapper = new ObjectMapper() ;
            SimpleModule module1 = new SimpleModule("JsonStructDeserializer") ;
            module1.addDeserializer(Object.class, new JsonStructDeserializer(Object.class)) ;
            customStructObjectMapper.registerModule(module1) ;
            try {
                this.map = customStructObjectMapper.readValue(data.toString(), new TypeReference<Object>(){});

                /*for (Map.Entry<String, Object> entry : jsonMap.entrySet())
                    System.out.println("Key = " + entry.getKey() +
                            ", Value = " + entry.getValue());
                System.out.println("Field name: " + f.getFieldName()) ;*/
                if (this.map.containsKey(f.getFieldName())) {
                    return this.map.get(f.getFieldName()) ;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null ;
        /*}
            List<Object> row = (List<Object>)data ;
            //System.out.println("This is being used") ;
           // System.out.println(f.getFieldName()) ;
            return row.get(getElementPosInList(fields, f.getFieldName())) ;*/

    }

    public int getElementPosInList(List<MyField> list, Object o) {
        for (int i = 0 ; i < list.size() ; i++) {
            //System.out.println(list.get(i)) ;
            if (list.get(i).getFieldName().equalsIgnoreCase(o.toString())) {
                return i ;
            }
        }
        return -1 ;
    }

}
