package org.mmi.serde.objectinspector.jsonparser;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sbhTWR
 */

/*
This class parses special kind of JSON format called "New Line delimited JSON or ndjson"
An example of ndjson to be used in our case is :

{"_id":{"$numberLong":"19118307445"},"accountId":{"$numberLong":"19574"},"deviceId":{"$numberLong":"50738"},"entityId":[{"$numberLong":"59876"}],"uniqueId":"864495034483997","timestamp":{"$numberLong":"1522607399"},"insertTime":{"$numberLong":"1522610931"},"longitude":79.943207,"latitude":12.831075,"heading":149.0,"speed":0.0,"hdop":0.67,"numberOfSatellites":15,"digitalInput1":0,"digitalInput2":1,"altitude":66.0,"powerSupplyVoltage":12400.0,"internalBatteryVoltage":4200.0,"power":1,"gsmlevel":14,"gpsSpeed":0.0,"valid":true,"gpsFix":true,"indianBox":false,"validGPS":false,"accOff":false,"type":0,"panic":1,"pdop":1.21,"processFlags":{"history":false},"movementStatus":"stopped","address":"Oragadam Industrial Area, Tamil Nadu. 442 m from Unipres India Pvt LTD (India)","currentZones":null,"intouchOdometer":2040.7569495043494,"createdAt":{"$date":"2018-04-01T19:28:51.000Z"},"day":1,"month":3,"year":2018}
{"_id":{"$numberLong":"19118307443"},"accountId":{"$numberLong":"19574"},"deviceId":{"$numberLong":"50738"},"entityId":[{"$numberLong":"59876"}],"uniqueId":"864495034483997","timestamp":{"$numberLong":"1522607394"},"insertTime":{"$numberLong":"1522610914"},"longitude":79.943207,"latitude":12.831075,"heading":149.0,"speed":0.0,"hdop":0.73,"numberOfSatellites":14,"digitalInput1":0,"digitalInput2":1,"altitude":66.0,"powerSupplyVoltage":12400.0,"internalBatteryVoltage":4200.0,"power":1,"gsmlevel":14,"gpsSpeed":0.0,"valid":true,"gpsFix":true,"indianBox":false,"validGPS":false,"accOff":false,"type":0,"panic":1,"pdop":1.28,"processFlags":{"history":false},"movementStatus":"stopped","address":"Oragadam Industrial Area, Tamil Nadu. 442 m from Unipres India Pvt LTD (India)","currentZones":null,"intouchOdometer":2040.7569495043494,"createdAt":{"$date":"2018-04-01T19:28:34.000Z"},"day":1,"month":3,"year":2018}

The parser can deserialize ndjson into java maps. To use this, the module needs to be added to the ObjectMapper followed by ObjectMapper.addValue()
An example usage is :

Main:
ObjectMapper customObjectMapper = new ObjectMapper() ;
        File jsonFile = new File("/home/shubham/Desktop/org.mmi.serde.objectinspector.jsonparser/test.json") ;
        SimpleModule module = new SimpleModule("org.mmi.serde.objectinspector.JsonSerDe") ;
        module.addDeserializer(Object.class, new JsonDeserializer(Object.class)) ;
        customObjectMapper.registerModule(module) ;
        List<String> jsonList = customObjectMapper.readValue(jsonFile, new TypeReference<Object>(){}) ;

        for (int i = 0 ; i < jsonList.size() ; i++) {
            System.out.println(jsonList.get(i)) ;
        }

        ObjectMapper objectMapper = new ObjectMapper() ;

        Map<String, Object> jsonMap = objectMapper.readValue(jsonList.get(1), // get(1) gets the second struct field parsed. We can change the integer and get any struct we want, or loop it to get all structs parsed.
                new TypeReference<Map<String,Object>>(){});

        for (Map.Entry<String, Object> entry : jsonMap.entrySet())
            System.out.println("Key:  " + entry.getKey() + ", Value: " + entry.getValue()) ;
 */

public class JsonDeserializer extends StdDeserializer<Object> {
    private com.fasterxml.jackson.core.JsonParser parser ;
    private JsonToken prevToken ;
    private JsonToken currentToken ;

    public JsonDeserializer(Class<?> vc) {
        super(vc) ;
        prevToken = JsonToken.NOT_AVAILABLE ;
        currentToken = JsonToken.NOT_AVAILABLE ;
        parser = null ;
    }

    @Override
    public Object deserialize(com.fasterxml.jackson.core.JsonParser parser, DeserializationContext ctxt) throws IOException {
        // implement custom parser here
        this.parser = parser ;
        List<String> rows = new ArrayList<String>() ;
        StringBuilder sb = new StringBuilder() ;
        do {
            currentToken = this.parser.getCurrentToken() ;

            if ((JsonToken.START_OBJECT.equals(currentToken)) && prevToken.equals(JsonToken.END_OBJECT) ) {
                // we have built a new struct, we need to append the string to the list and clear it
               // System.out.println(sb.toString()) ;
                //this.parser.getCurrentLocation() ;
                rows.add(sb.toString()) ;
                sb.setLength(0);
            }
            // append the new token to the string anyway
            sb = append(sb) ;
            prevToken = this.parser.getCurrentToken() ;
            this.parser.nextToken() ;
            if (this.parser.isClosed()) {
                sb = append(sb) ;
                rows.add(sb.toString()) ;
               // System.out.println(sb.toString()) ;
            }

        } while(!this.parser.isClosed()) ;
        return rows ;
        /* List<Long> offsets = new ArrayList<Long>() ;
        StringBuilder sb = new StringBuilder() ;
        do {
            jsonToken = parser.getCurrentToken() ;

            if ((JsonToken.START_OBJECT.equals(jsonToken)) && prevToken.equals(JsonToken.END_OBJECT) ) {
                // we have built a new struct, we need to append the string to the list and clear it
                // System.out.println(sb.toString()) ;
                offsets.add(parser.getCurrentLocation().getCharOffset()) ;

            }
            // append the new token to the string anyway
            prevToken = parser.getCurrentToken() ;
            parser.nextToken() ;
            if (parser.isClosed()) {
                offsets.add(parser.getCurrentLocation().getCharOffset()) ;
                // System.out.println(sb.toString()) ;
                parser.
            }
        } while(!parser.isClosed()) ;
        return offsets ;*/
    }


    private StringBuilder append(StringBuilder sb) throws IOException {


        if (prevToken.equals(JsonToken.FIELD_NAME) && currentToken.equals(JsonToken.VALUE_STRING)) {
            sb.append(":\"" + parser.getText() + "\"") ;
        }

        else if (prevToken.equals(JsonToken.FIELD_NAME) && ((
                   currentToken.equals(JsonToken.VALUE_FALSE)
                || currentToken.equals(JsonToken.VALUE_TRUE)
                || currentToken.equals(JsonToken.VALUE_NULL)
                || currentToken.equals(JsonToken.VALUE_NUMBER_INT)
                || currentToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                || currentToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
        ))) {
            sb.append(":" + parser.getText()) ;
        }
        else if (prevToken.equals(JsonToken.END_OBJECT) && currentToken.equals(JsonToken.FIELD_NAME)) {
                sb.append(",\"" + parser.getText() + "\"") ;

        }
        else if (prevToken.equals(JsonToken.START_OBJECT) && currentToken.equals(JsonToken.FIELD_NAME)) {
            sb.append("\"" + parser.getText() + "\"") ;
        }

        /*else if (( prevToken.equals(JsonToken.VALUE_STRING)
                || prevToken.equals(JsonToken.VALUE_FALSE)
                || prevToken.equals(JsonToken.VALUE_TRUE)
                || prevToken.equals(JsonToken.VALUE_NULL)
                || prevToken.equals(JsonToken.VALUE_NUMBER_INT)
                || prevToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                || prevToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
                )) {
            if (currentToken.equals(JsonToken.VALUE_STRING)) {
                sb.append(",\"" + parser.getText() + "\"");
            } else if (( currentToken.equals(JsonToken.VALUE_STRING)
                    || currentToken.equals(JsonToken.VALUE_FALSE)
                    || currentToken.equals(JsonToken.VALUE_TRUE)
                    || currentToken.equals(JsonToken.VALUE_NULL)
                    || currentToken.equals(JsonToken.VALUE_NUMBER_INT)
                    || currentToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                    || currentToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
                    )) {
                sb.append("," + parser.getText()) ;
            }
            }*/
        else if (prevToken.equals(JsonToken.FIELD_NAME) && (currentToken.equals(JsonToken.START_OBJECT) || currentToken.equals(JsonToken.START_ARRAY))) {
            sb.append(":" + parser.getText()) ;
        }

        else if (prevToken.equals(JsonToken.END_ARRAY) && (currentToken.equals(JsonToken.FIELD_NAME))) {
            sb.append(",\"" + parser.getText() + "\"") ;
        }

        else if (( prevToken.equals(JsonToken.VALUE_STRING)
                || prevToken.equals(JsonToken.VALUE_FALSE)
                || prevToken.equals(JsonToken.VALUE_TRUE)
                || prevToken.equals(JsonToken.VALUE_NULL)
                || prevToken.equals(JsonToken.VALUE_NUMBER_INT)
                || prevToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                || prevToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
        ) && currentToken.equals(JsonToken.FIELD_NAME)) {
            sb.append(",\"" + parser.getText() + "\"") ;
        }
        else {
            sb.append(parser.getText()) ;
        }

        return sb ;
    }
}


