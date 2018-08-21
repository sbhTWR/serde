package com.mapmyindia.serde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector ;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;

import com.mapmyindia.serde.objectinspector.primitive.* ;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sbhTWR
 */

public final class JsonObjectInspectorFactory {

    //to prevent instantiation, we throw an exception.
    private JsonObjectInspectorFactory() throws InstantiationException {
        throw new InstantiationException("Factory class should not be instantiated.") ;
    }

    public static ObjectInspector getJsonObjectInspectorFromTypeInfo(TypeInfo typeInfo)
            throws CategoryNotFoundException {
        ObjectInspector result ;
        switch (typeInfo.getCategory()) {
            // all JSON types are supported by only three Categories of data: primitive, list and map.
            case PRIMITIVE: {
                PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo ;
                result = JsonObjectInspectorFactory.getPrimitiveJsonObjectInspector(pti.getPrimitiveCategory()) ;
                break ;
            }
            case LIST:  {
            /* A list in java is homogenous, that is, all its elements have the same datatype. Since the TypeInfo
            represents a list, we can down-cast it to TypeInfo safely and get an elementObjectInspector which is
            same for elements, and call getJsonListObjectInspector(elementObjectInspector)
             */
                ObjectInspector elementObjectInspector
                        = getJsonObjectInspectorFromTypeInfo(((ListTypeInfo)typeInfo).getListElementTypeInfo()) ;
                result = JsonObjectInspectorFactory.getJsonListObjectInspector(elementObjectInspector) ;
                break ;
            }
            case MAP: {
                // we know that typeInfo represents map, we can safely down-cast it to MapTypeInfo
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo ;
                ObjectInspector keyObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo()) ;
                ObjectInspector valueObjectInspector
                        = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo()) ;
                result = JsonObjectInspectorFactory.getJsonMapObjectInspector(keyObjectInspector,valueObjectInspector) ;
                break ;
            }

            case STRUCT: {
                StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo ;
                List<String> fieldNames = structTypeInfo.getAllStructFieldNames() ;
                List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos() ;
                List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fieldTypeInfos.size()) ;
                for (int i = 0 ; i < fieldTypeInfos.size(); i++) {
                    fieldObjectInspectors.add(getJsonObjectInspectorFromTypeInfo(fieldTypeInfos.get(i))) ;
                }
                result = JsonObjectInspectorFactory.getJsonStructObjectInspector(fieldNames,fieldObjectInspectors) ;
                break ;
            }
            default:{
                throw new CategoryNotFoundException("Category : " + typeInfo.getCategory() + "not found.") ;
            }

        }
        return result ;
    }

    public static JsonStructObjectInspector getJsonStructObjectInspector(
            List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors) {
        return new JsonStructObjectInspector(structFieldNames, structFieldObjectInspectors) ;
    }

    public static AbstractPrimitiveJavaObjectInspector getPrimitiveJsonObjectInspector (PrimitiveCategory pCat)
            throws CategoryNotFoundException {
        AbstractPrimitiveJavaObjectInspector result ;
        switch(pCat) {
            case INT: {
                result = new JsonIntObjectInspector();
                break;
            }
            case DOUBLE: {
                result = new JsonDoubleObjectInspector();
                break;
            }
            case STRING: {
                result = new JsonStringObjectInspector() ;
                break ;
            }
            case BOOLEAN: {
                result = new JsonBooleanObjectInspector();
                break;
            }
            default: {
                throw new CategoryNotFoundException("No such category: " + pCat.toString() + "defined.") ;
            }

        }
        return result ;
    }

    public static JsonMapObjectInspector getJsonMapObjectInspector(
            ObjectInspector mapKeyObjectInspector,
            ObjectInspector mapValueObjectInspector) {
        return new JsonMapObjectInspector(mapKeyObjectInspector, mapValueObjectInspector) ;
    }

    public static JsonListObjectInspector getJsonListObjectInspector(
            ObjectInspector listElementObjectInspector) {
        return new JsonListObjectInspector(listElementObjectInspector) ;
    }

}


