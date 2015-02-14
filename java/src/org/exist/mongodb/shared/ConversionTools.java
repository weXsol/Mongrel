package org.exist.mongodb.shared;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.XPathUtil;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;
import org.exist.xquery.value.ValueSequence;

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

        BasicDBObject retVal;

        if (seq.getItemType() == Type.STRING) {
            // Do direct conversion
            retVal = convertJSon(seq.getStringValue());
        } else {
            retVal = (BasicDBObject) parseSequence(seq);
        }

        return retVal;
    }

    public static Object parseSequence(Sequence seq) throws XPathException {

        Object retVal;

        switch (seq.getItemType()) {
            case Type.MAP:
                MapType map = (MapType) seq;
                //String key = map.getKey().getStringValue();
                BasicDBObject value = parseMap(map);
                retVal = value;
                break;
            case Type.ARRAY:
                ArrayType array = (ArrayType) seq;
                ArrayList values = parseArray(array);
                retVal = values;
                break;
            default:
                retVal = ConversionTools.convertParameters(seq)[0];
        }

        return retVal;
    }

    private static BasicDBObject parseMap(MapType map) throws XPathException {
        BasicDBObject retVal = new BasicDBObject();

        for (Map.Entry<AtomicValue, Sequence> mapEntry : map) {

            // A key is always a string
            String key = mapEntry.getKey().getStringValue();

            // The value is always a sequence
            Sequence sequence = mapEntry.getValue();

            // Recurivele add value
            retVal.append(key, parseSequence(sequence));
        }

        return retVal;
    }

    private static ArrayList parseArray(ArrayType array) throws XPathException {

        ArrayList<Object> retVal = new ArrayList();

        for (Sequence sequence : array.toArray()) {
            // Recurively add value
            retVal.add(parseSequence(sequence));
        }

        return retVal;
    }

    public static Sequence convertBson(XQueryContext context, BasicDBObject bson) throws XPathException {

        Sequence retVal = new ValueSequence();
        MapType mt = new MapType(context);

        for (Map.Entry<String, Object> entry : bson.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            mt.add(new StringValue(key), getValues(context, value));

        }
        retVal.add(mt);
        return retVal;
    }

    public static Sequence getValues(XQueryContext context, Object o) throws XPathException {

        Sequence retVal = new ValueSequence();

        if (o instanceof BasicDBObject) {

            BasicDBObject bson = (BasicDBObject) o;
            MapType mt = new MapType(context);

            for (Map.Entry<String, Object> entry : bson.entrySet()) {

                String key = entry.getKey();
                Object value = entry.getValue();

                mt.add(new StringValue(key), getValues(context, value));

            }
            retVal.add(mt);

        } else if (o instanceof BasicDBList) {
            BasicDBList list = (BasicDBList) o;

            List<Sequence> collected = new ArrayList();

            for (Object item : list) {
                collected.add(getValues(context, item));
            }

            ArrayType at = new ArrayType(context, collected);
            retVal.add(at);

        } else {
            // regular javaobject
            // TODO : create actual objects
            retVal = XPathUtil.javaObjectToXPath(o, context);
        }

        return retVal;

    }

    /**
     * Convert Sequence into array of Java objects
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
