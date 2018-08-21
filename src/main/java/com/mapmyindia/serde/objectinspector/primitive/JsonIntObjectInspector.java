package com.mapmyindia.serde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;


public class JsonIntObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableIntObjectInspector {

    public JsonIntObjectInspector() {
        super(TypeInfoFactory.intTypeInfo) ;
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if (o == null) return null ;

        if (o instanceof String) {
            return new IntWritable(Integer.parseInt((String)o)) ;
        } else {
            return new IntWritable((Integer)o) ;
        }
    }

    @Override
    public int get(Object o) {
        if (o instanceof String) {
            return Integer.parseInt((String) o) ;
        } else {
            return (Integer)o ;
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        return get(o) ;
    }

    @Override
    public Object create(int value) {
        return value ;
    }

    @Override
    public Object set(Object o, int value) {
        return value ;
    }
}
