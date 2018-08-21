package com.mapmyindia.serde.objectinspector.jsonparser ;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses the JSON data into a list.
 *
 * @author shubham
 */

public class JsonListDeserializer extends StdDeserializer<Object> {
    private JsonParser parser ;
    private JsonToken prevToken ;
    private JsonToken currentToken ;

    public JsonListDeserializer(Class<?> vc) {
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
        List<Object> jsonList = new ArrayList<Object>() ;
        currentToken = this.parser.getCurrentToken() ;
        if (!(currentToken == JsonToken.START_ARRAY)) {
            System.out.println("Expected [") ;
            return null ; // throw some error later
        }
        prevToken = this.parser.getCurrentToken() ;
        currentToken = this.parser.nextToken() ;
        //=========================================================================================================
        do {
            if ((JsonToken.END_ARRAY == currentToken)) {
                break ;
            }
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
                sb = append(sb) ;
            }
            // set sb length to zero and add to Map
            jsonList.add(sb.toString()) ;
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
        return jsonList ;

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




