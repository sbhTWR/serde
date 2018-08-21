package com.mapmyindia.serde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable ;

public class JsonDoubleObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableDoubleObjectInspector {

    public JsonDoubleObjectInspector() {
        super(TypeInfoFactory.doubleTypeInfo) ;
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if (o == null) return null ;

        if (o instanceof String) {
            return new DoubleWritable(Double.parseDouble((String)o)) ;
        } else {
            return new DoubleWritable((Double)o) ;
        }
    }

    @Override
    public double get(Object o) {
        if (o instanceof String) {
            return Double.parseDouble((String)o) ;
        } else {
            return (Double)o ;
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        return get(o) ;
    }

    @Override
    public Object create(double value) {
        return value ;
    }

    @Override
    public Object set(Object o, double value) {
        return value ;
    }

}
