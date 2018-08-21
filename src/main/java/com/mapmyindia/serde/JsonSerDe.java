package com.mapmyindia.serde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.mapmyindia.serde.objectinspector.CategoryNotFoundException;
import com.mapmyindia.serde.objectinspector.JsonObjectInspectorFactory;

import java.util.*;

/**
 * JsonDeserializer reads data from a JSON file and parses them into Java objects (deserialize).
 * ObjectInspectors are used to get the hive row data from the java objects. The ObjectInspector categories
 * required in our case are STRUCT, LIST, PRIMITIVE. Only four datatypes are supported in category PRIMITIVE
 * viz. int, double, string and boolean.
 *
 * @author sbhTWR
 */

public class JsonSerDe extends AbstractSerDe {

    public static final Log LOG = LogFactory.getLog(JsonSerDe.class) ;
    StructObjectInspector rowObjectInspector ;
    ObjectMapper objectMapper ;
    List<String> colNames ;
    List<TypeInfo> colTypes ;
    StructTypeInfo rowTypeInfo ;


    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {

        LOG.debug("Initializing Custom Deserializer") ;
        //objectMapper = new ObjectMapper();
        String colNamesCSV = tbl.getProperty(serdeConstants.LIST_COLUMNS) ;
        String colTypesCSV = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES) ;

        // we have got CSV names and types of columns, now we need to parse these values into a list using ',' as a delimiter.
        // Logging the CSV for debugging

        LOG.debug("serdeConstants.LIST_COLUMNS: " + colNamesCSV) ;
        LOG.debug("serdeConstants.LIST_COLUMN_TYPES: " + colTypesCSV) ;

        // parse the CSV
        if (colNamesCSV.length()!=0)
            colNames = Arrays.asList(colNamesCSV.split(",")) ;
        else colNames = new ArrayList<String>() ;

        if (colTypesCSV.length()!=0)
            colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesCSV);
        else colNames = new ArrayList<String>() ;

        assert(colNames.size()==colTypes.size()) ;

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        
        try {
            rowObjectInspector =
                    (StructObjectInspector) JsonObjectInspectorFactory.getJsonObjectInspectorFromTypeInfo(rowTypeInfo);
        } catch (CategoryNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        throw new UnsupportedOperationException("Unsupported Operation: serializing is not supported!") ;
    }

    @Override
    public Writable serialize(java.lang.Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException("Unsupported Operation: serialize() is not supported!") ;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null ;
    }

    @Override
    public java.lang.Object deserialize(Writable blob) throws SerDeException {
        // convert the data into String, which can be parsed through readValue of jackson
        Text jsonData = (Text)blob ;
        String txt = jsonData.toString() ;
        return txt ;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjectInspector ;
    }
}
