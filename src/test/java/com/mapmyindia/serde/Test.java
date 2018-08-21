package com.mapmyindia.serde;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Test {

    public void setUp() throws Exception {
        initialize();
    }

    static public void initialize() {

    }

    public void test1() throws Exception {
        JsonSerDe serde = new JsonSerDe() ;
        Configuration conf = null ;
        Properties tbl = new Properties() ;
        tbl.setProperty(serdeConstants.LIST_COLUMNS,"_id,accountId,deviceId,entityId,uniqueId,timestamp,insertTime,longitude,latitude,heading,speed,hdop,numberOfSatellites,digitalInput1,digitalInput2,altitude,powerSupplyVoltage,internalBatteryVoltage,power,gsmlevel,gpsSpeed,valid,gpsFix,indianBox,validGPS,accOff,type,panic,pdop,processFlags,movementStatus,address,currentZones,intouchOdometer,createdAt,day,month,year") ;
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("struct<$numberLong:string>,struct<$numberLong:string>,struct<$numberLong:string>,array<struct<$numberLong:string>>,string,struct<$numberLong:string>,struct<$numberLong:string>,double,double,int,int,double,int,int,int,int,int,int,int,int,int,boolean,boolean,boolean,boolean,boolean,int,int,double,struct<history:boolean>,string,string,string,double,struct<$date:string>,int,int,int")) ;
        serde.initialize(conf,tbl);
        deserializeAndGetValues(serde);
    }

  /*  public void test2() throws Exception {
        JsonSerDe serde = new JsonSerDe() ;
        Configuration conf = null ;
        Properties tbl = new Properties() ;
        tbl.setProperty(serdeConstants.LIST_COLUMNS,"name,nest") ;
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("string,struct<lol:array<int>>")) ;
        serde.initialize(conf,tbl);
        deserializeAndGetValues(serde);
    }*/

    public static void deserializeAndGetValues(JsonSerDe serde) throws SerDeException {
        // initialize a test string
        Writable w = new Text("{\"_id\":{\"$numberLong\":\"19118307445\"},\"accountId\":{\"$numberLong\":\"19574\"},\"deviceId\":{\"$numberLong\":\"50738\"},\"entityId\":[{\"$numberLong\":\"59876\"}],\"uniqueId\":\"864495034483997\",\"timestamp\":{\"$numberLong\":\"1522607399\"},\"insertTime\":{\"$numberLong\":\"1522610931\"},\"longitude\":79.943207,\"latitude\":12.831075,\"heading\":149.0,\"speed\":0.0,\"hdop\":0.67,\"numberOfSatellites\":15,\"digitalInput1\":0,\"digitalInput2\":1,\"altitude\":66.0,\"powerSupplyVoltage\":12400.0,\"internalBatteryVoltage\":4200.0,\"power\":1,\"gsmlevel\":14,\"gpsSpeed\":0.0,\"valid\":true,\"gpsFix\":true,\"indianBox\":false,\"validGPS\":false,\"accOff\":false,\"type\":0,\"panic\":1,\"pdop\":1.21,\"processFlags\":{\"history\":false},\"movementStatus\":\"stopped\",\"address\":\"Oragadam Industrial Area, Tamil Nadu. 442 m from Unipres India Pvt LTD (India)\",\"currentZones\":null,\"intouchOdometer\":2040.7569495043494,\"createdAt\":{\"$date\":\"2018-04-01T19:28:51.000Z\"},\"day\":1,\"month\":3,\"year\":2018}") ;

        Object row = null;
        row = serde.deserialize(w);
        StructObjectInspector soi = (StructObjectInspector)serde.getObjectInspector() ;
        System.out.println(soi.getStructFieldData(row, soi.getStructFieldRef("entityId"))) ;

        // now the elements inside
        StructField sflang = soi.getStructFieldRef("entityId") ;
        ListObjectInspector loi = (ListObjectInspector) sflang.getFieldObjectInspector() ;
        Object val = soi.getStructFieldData(row, sflang) ;
        System.out.println(loi.getListElement(val,0)) ;

        StructObjectInspector structinsidearrayOI = (StructObjectInspector) loi.getListElementObjectInspector() ;
        System.out.println(structinsidearrayOI.getStructFieldData(loi.getListElement(val,0),structinsidearrayOI.getStructFieldRef("$numberlong"))) ;

    }

}