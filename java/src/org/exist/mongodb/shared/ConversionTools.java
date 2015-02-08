package org.exist.mongodb.shared;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.exist.xquery.XPathException;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.Type;

/**
 *
 * @author wessels
 */
public class ConversionTools {
    
    protected final static Logger LOG = Logger.getLogger(ConversionTools.class);
    

    /**
     * Convert sequence of strings into List of DBobjects
     *
     * @param args The input sequence.
     * @return List of BSON objects.
     *
     * @throws XPathException exist data conversion failed.
     * @throws JSONParseException A string could not be parsed.
     */
    public static List<DBObject> convertPipeline(Sequence args) throws XPathException, JSONParseException {

        List<DBObject> pipeline = new ArrayList();

        if (args != null) {
            SequenceIterator iterator = args.iterate();
            while (iterator.hasNext()) {
                Item next = iterator.nextItem();

                String step = next.getStringValue();

                pipeline.add((BasicDBObject) JSON.parse(step));

            }

        }

        return pipeline;
    }

    /**
     * Convert JSON string to BSON object
     *
     * @param json The json string
     * @return the converted result, null if input is null.
     * @throws JSONParseException The string could not be parsed.
     */
    public static BasicDBObject convertJSon(String json) throws JSONParseException {
        return (json == null) ? null : (BasicDBObject) JSON.parse(json);
    }

    /**
     * Convert first mapEntry in sequence to BSON object
     *
     * @param seq The sequence
     * @return the converted result, null if input is null.
     * @throws JSONParseException The string could not be parsed.
     * @throws XPathException The string value could not be obtained
     */
    public static BasicDBObject convertJSon(Sequence seq) throws JSONParseException, XPathException {

        BasicDBObject retVal = new BasicDBObject();
        
        if(seq.getItemType()==Type.STRING){
            retVal = convertJSon(seq.getStringValue());
        } else {
            parseSequence(seq, retVal, null);
        }

        return retVal;
    }

    public static void parseSequence(Sequence seq, BasicDBObject bson, String key) throws XPathException {
        switch (seq.getItemType()) {
            case Type.MAP:
                MapType map = (MapType) seq;
                parseMap(map, bson);
                break;
            case Type.ARRAY:
                ArrayType array = (ArrayType) seq;
                parseArray(array, bson, key);
                break;
            default:
                bson.append(key, ConversionTools.convertParameters(seq)[0]); // need for real converter
        }
    }

    private static void parseMap(MapType map, BasicDBObject bson) throws XPathException {

        for (Map.Entry<AtomicValue, Sequence> mapEntry : map) {
            
            // A key is always a string
            String key = mapEntry.getKey().getStringValue();
            
            // The value is always a sequence
            Sequence sequence = mapEntry.getValue();

            if (sequence.getItemType() == Type.MAP) {
                BasicDBObject newBson = new BasicDBObject();
                bson.append(key, newBson);
                parseMap((MapType)sequence, newBson);
                
            } else if (sequence.getItemType() == Type.ARRAY) {
                parseArray((ArrayType) sequence, bson, key);
                               
            } else {
                Object value = ConversionTools.convertParameters(sequence)[0];
                bson.append(key, value);
            }

        }

    }

    private static void parseArray(ArrayType array, BasicDBObject bson, String key) throws XPathException {
        
          
        for(Sequence sequence : array.toArray()) {

            if (sequence.getItemType() == Type.MAP) {
                BasicDBObject newBson = new BasicDBObject();
                bson.append(key, newBson);
                parseMap((MapType)sequence, newBson);
                
            } else if (sequence.getItemType() == Type.ARRAY) {
                parseArray((ArrayType) sequence, bson, key);
                               
            } else {
                Object value = ConversionTools.convertParameters(sequence)[0];
                bson.append(key, value);
            }
            
            
        }
        

    }
    
     /**
     *  Convert Sequence into array of Java objects
     */
    public static Object[] convertParameters(Sequence args) throws XPathException {
        List<Object> params = new ArrayList<>();
        SequenceIterator iterate = args.iterate();
        while (iterate.hasNext()) {
            
            Item item = iterate.nextItem();
            
            switch (item.getType()) {
                case Type.STRING:
                    params.add(item.getStringValue());
                    break;
                               
                case Type.INTEGER:
                case Type.INT:
                    params.add(item.toJavaObject(Integer.class));
                    break;
                    
                case Type.DOUBLE:
                    params.add(item.toJavaObject(Double.class));
                    break;
                    
                case Type.BOOLEAN:
                    params.add(item.toJavaObject(Boolean.class));
                    break;
                    
                case Type.DATE_TIME:                  
                    params.add(item.toJavaObject(Date.class));
                    break;
                    
                default:
                    LOG.info(String.format("Fallback: Converting '%s' to String value", Type.getTypeName(item.getType())));
                    params.add(item.getStringValue());
                    break;
            }
            
        }
        return params.toArray();
    }

}
