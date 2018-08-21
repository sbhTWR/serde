package com.mapmyindia.serde.objectinspector.jsonparser ;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses the JSON data into a map.
 *
 * @author sbhTWR
 */

public class JsonStructDeserializer extends StdDeserializer<Object> {
    private JsonParser parser ;
    private JsonToken prevToken ;
    private JsonToken currentToken ;

    public JsonStructDeserializer(Class<?> vc) {
        super(vc) ;
        prevToken = JsonToken.NOT_AVAILABLE ;
        currentToken = JsonToken.NOT_AVAILABLE ;
        parser = null ;
    }

    @Override
    public Object deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
        // implement custom parser here
        this.parser = parser ;
        StringBuilder sb = new StringBuilder() ;
        Map<String, Object> jsonMap = new HashMap<String, Object>();

        currentToken = this.parser.getCurrentToken() ;
        if (!(currentToken == JsonToken.START_OBJECT)) {
            System.out.println("Expected {") ;
            return null ; // throw some error later
        }
        prevToken = this.parser.getCurrentToken() ;
        currentToken = this.parser.nextToken() ;
        do {
            if (!(JsonToken.FIELD_NAME == currentToken) && !(JsonToken.END_OBJECT == currentToken)) {
                System.out.println("Error, expected field name but got " + currentToken.toString() + "prevToken: " + prevToken.toString()) ;
                return null ;
            }
            if ((JsonToken.END_OBJECT == currentToken)) {
                break ;
            }
            String currentField = parser.getText() ;
            prevToken = currentToken ;
            currentToken = parser.nextToken() ;
            if (JsonToken.START_OBJECT == currentToken) {
                sb.append("{") ;
                int obj = 1 ;
                while (obj!=0) {
                    prevToken = currentToken;
                    currentToken = this.parser.nextToken();
                    if (currentToken == JsonToken.END_OBJECT) obj-- ;
                    if (currentToken == JsonToken.START_OBJECT) obj++ ;
                    sb = append(sb) ;
                }

            } else if (JsonToken.START_ARRAY == currentToken) {
                sb.append("[") ;
                int obj = 1 ;
                while (obj!=0) {
                    prevToken = currentToken;
                    currentToken = this.parser.nextToken();
                    if (currentToken == JsonToken.END_ARRAY) obj-- ;
                    if (currentToken == JsonToken.START_ARRAY) obj++ ;
                    sb = append(sb) ;
                }

            } else if (( currentToken.equals(JsonToken.VALUE_STRING)
                    || currentToken.equals(JsonToken.VALUE_FALSE)
                    || currentToken.equals(JsonToken.VALUE_TRUE)
                    || currentToken.equals(JsonToken.VALUE_NULL)
                    || currentToken.equals(JsonToken.VALUE_NUMBER_INT)
                    || currentToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                    || currentToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
            )) {
                //System.out.println("-->" + parser.getText() + "<--") ;
                //currentToken.equals(JsonToken.VALUE_STRING)? sb.append("\"" + this.parser.getText() + "\""): sb.append(this.parser.getText())  ;
                if (currentToken.equals(JsonToken.VALUE_STRING)) {
                    sb.append("\"" + this.parser.getText() + "\"") ;
                } else {
                    sb.append(this.parser.getText()) ;
                }

            }
            // set sb length to zero and add to Map
            jsonMap.put(currentField.toLowerCase(), sb.toString()) ;
            sb.setLength(0);
            //================================================
            prevToken = this.parser.getCurrentToken() ;
            currentToken = this.parser.nextToken() ;
            if (this.parser.isClosed()) {
                sb = append(sb) ;
                // System.out.println(sb.toString()) ;
            }
        }
        while (!this.parser.isClosed()) ;
        return jsonMap ;
       /* List<String> rows = new ArrayList<String>() ;
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
        */
        //=================================================================================================
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
        //===================================================================================================
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
            sb.append(",\"" + parser.getText().toLowerCase() + "\"") ;

        }
        else if (prevToken.equals(JsonToken.START_OBJECT) && currentToken.equals(JsonToken.FIELD_NAME)) {
            sb.append("\"" + parser.getText().toLowerCase() + "\"") ;
        }

        else if (( prevToken.equals(JsonToken.VALUE_STRING)
                || prevToken.equals(JsonToken.VALUE_FALSE)
                || prevToken.equals(JsonToken.VALUE_TRUE)
                || prevToken.equals(JsonToken.VALUE_NULL)
                || prevToken.equals(JsonToken.VALUE_NUMBER_INT)
                || prevToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                || prevToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
        ) && currentToken.equals(JsonToken.VALUE_STRING)) {

            sb.append(",\"" + parser.getText() + "\"");
        }
        else if (
                (currentToken.equals(JsonToken.VALUE_FALSE)
                        || currentToken.equals(JsonToken.VALUE_TRUE)
                        || currentToken.equals(JsonToken.VALUE_NULL)
                        || currentToken.equals(JsonToken.VALUE_NUMBER_INT)
                        || currentToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                        || currentToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
                ) && ( prevToken.equals(JsonToken.VALUE_STRING)
                        || prevToken.equals(JsonToken.VALUE_FALSE)
                        || prevToken.equals(JsonToken.VALUE_TRUE)
                        || prevToken.equals(JsonToken.VALUE_NULL)
                        || prevToken.equals(JsonToken.VALUE_NUMBER_INT)
                        || prevToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                        || prevToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
                )) {
            sb.append("," + parser.getText()) ;
        }

        else if (prevToken.equals(JsonToken.FIELD_NAME) && (currentToken.equals(JsonToken.START_OBJECT) || currentToken.equals(JsonToken.START_ARRAY))) {
            sb.append(":" + parser.getText()) ;
        }

        else if (prevToken.equals(JsonToken.END_ARRAY) && (currentToken.equals(JsonToken.FIELD_NAME))) {
            sb.append(",\"" + parser.getText().toLowerCase() + "\"") ;
        }

        else if (( prevToken.equals(JsonToken.VALUE_STRING)
                || prevToken.equals(JsonToken.VALUE_FALSE)
                || prevToken.equals(JsonToken.VALUE_TRUE)
                || prevToken.equals(JsonToken.VALUE_NULL)
                || prevToken.equals(JsonToken.VALUE_NUMBER_INT)
                || prevToken.equals(JsonToken.VALUE_NUMBER_FLOAT)
                || prevToken.equals(JsonToken.VALUE_EMBEDDED_OBJECT)
        ) && currentToken.equals(JsonToken.FIELD_NAME)) {
            sb.append(",\"" + parser.getText().toLowerCase() + "\"") ;
        } else if (prevToken.equals(JsonToken.END_OBJECT) && (currentToken.equals(JsonToken.START_OBJECT))) {
            sb.append(",{") ;
        }
        else {
            sb.append(parser.getText()) ;
        }

        return sb ;
    }
}


