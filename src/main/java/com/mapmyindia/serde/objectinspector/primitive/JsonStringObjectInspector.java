package com.mapmyindia.serde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text ;


public class JsonStringObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableStringObjectInspector {

    public JsonStringObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo) ;
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
        if (o == null) return null ;

        return new Text((String)o) ;
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
        return o.toString() ;
    }

    @Override
    public Object create(Text value) {
        if (value == null) return null ;
        return value.toString() ;
    }

    @Override
    public Object set(Object o, Text value) {
        if (value == null) return null ;
        return value.toString() ;
    }

    @Override
    public Object create(String value) {
        return value ;
    }

    @Override
    public Object set(Object o, String value) {
        return value ;
    }


}
