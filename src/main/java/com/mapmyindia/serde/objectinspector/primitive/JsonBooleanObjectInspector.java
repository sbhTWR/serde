package com.mapmyindia.serde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;


public class JsonBooleanObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableBooleanObjectInspector {

    public JsonBooleanObjectInspector() {

        super(TypeInfoFactory.booleanTypeInfo) ;
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if (o == null) return null ;

        if (o instanceof String) {
            return new BooleanWritable(Boolean.parseBoolean((String)o)) ;
        } else {
            return new BooleanWritable((Boolean)o) ;
        }
    }

    @Override
    public boolean get(Object o) {
        if (o instanceof String) {
            return Boolean.parseBoolean((String)o) ;
        } else {
            return (Boolean)o ;
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        return get(o) ;
    }

    @Override
    public Object create(boolean value) {
        return value ;
    }

    @Override
    public Object set(Object o, boolean value) {
        return value ;
    }
}

