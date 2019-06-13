package org.exist.mongodb.shared;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSONParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wessels
 */
public class ConversionTools {

    protected final static Logger LOG = LogManager.getLogger(ConversionTools.class);

    /**
     * Convert sequence of strings into List of DBobjects
     *
     * @param args The input sequence.
     * @return List of BSON objects.
     * @throws XPathException     exist data conversion failed.
     * @throws JSONParseException A string could not be parsed.
     */
    public static List<BasicDBObject> convertPipeline(final Sequence args) throws XPathException, JSONParseException {

        final List<BasicDBObject> pipeline = new ArrayList<>();

        if (args != null) {
            final SequenceIterator iterator = args.iterate();
            while (iterator.hasNext()) {
                final Item next = iterator.nextItem();

                if (next instanceof Sequence) { // Dead code
                    pipeline.add(MapToBSON.convert((Sequence)next));
                } else {
                    final String step = next.getStringValue();
                    pipeline.add(BasicDBObject.parse(step));
                }

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
    public static BasicDBObject convertJSon(final String json) throws JSONParseException {
        return (json == null) ? null : BasicDBObject.parse(json);
    }

//    /**
//     * Convert first mapEntry in sequence to BSON object
//     *
//     * @param seq The sequence
//     * @return the converted result, null if input is null.
//     * @throws JSONParseException The string could not be parsed.
//     * @throws XPathException     The string value could not be obtained
//     */
//    public static BasicDBObject convertJSon(Sequence seq) throws JSONParseException, XPathException {
//
//        return (seq.getItemType() == Type.STRING)
//                ? convertJSon(seq.getStringValue())
//                : (BasicDBObject) parseSequence(seq); // is this correct
//    }

//    /**
//     * Get simple BasicDBObject from sequence
//     *
//     * @param seq The input sequence
//     * @return The JSON representation of the sequence
//     * @throws JSONParseException The input could not be parsed
//     * @throws XPathException     The conversion failed.
//     */
//    public static BasicDBObject convertJSonParameter(Sequence seq) throws JSONParseException, XPathException {
//
//        if (seq == null || seq.isEmpty()) {
//            throw new IllegalArgumentException("Sequence is NULL or is empty.");
//        }
//
//        BasicBSONObject value = convertJSon(seq);
//
//        if (value == null) {
//            throw new IllegalArgumentException("Sequence cannot be converted to BasicDBObject (null value).");
//        }
//
//
//        if (!(value instanceof BasicDBObject)) {
//            throw new IllegalArgumentException("Sequence cannot be converted to BasicDBObject.");
//        }
//
//        return (BasicDBObject) value;
//    }

//    /**
//     * Convert a sequence into BSON type or Arraylist.
//     *
//     * @param seq The sequence.
//     * @return The conversion result.
//     * @throws XPathException Thrown when a conversion fails.
//     */
//    public static Object parseSequence(Sequence seq) throws XPathException {
//
//        Object retVal;
//
//        switch (seq.getItemType()) {
//            case Type.MAP:
//                MapType map = (MapType) seq;
//                retVal = parseMap(map);
//                break;
//            case Type.ARRAY:
//                ArrayType array = (ArrayType) seq;
//                retVal = parseArray(array);
//                break;
//            default:
//                retVal = ConversionTools.convertParameters(seq);
//        }
//
//        return retVal;
//    }

//    private static BasicDBObject parseMap(MapType map) throws XPathException {
//        BasicDBObject retVal = new BasicDBObject();
//
//        for (Map.Entry<AtomicValue, Sequence> mapEntry : map) {
//
//            // A key is always a string
//            String key = mapEntry.getKey().getStringValue();
//
//            // The value is always a sequence
//            Sequence sequence = mapEntry.getValue();
//
//            // Recurivele add value
//            retVal.append(key, parseSequence(sequence));
//        }
//
//        return retVal;
//    }

//    private static ArrayList parseArray(ArrayType array) throws XPathException {
//
//        ArrayList<Object> retVal = new ArrayList<>();
//
//        for (Sequence sequence : array.toArray()) {
//            // Recursively add value
//            retVal.add(parseSequence(sequence));
//        }
//
//        return retVal;
//    }

//    /**
//     * Convert an MongoDB BSON object into a sequence of xquery objects.
//     *
//     * @param context XQUery context
//     * @param bson    The BSON data
//     * @return The result of the conversion
//     * @throws XPathException Thrown when an xpath variable operation failed.
//     */
//    public static Sequence convertBson(XQueryContext context, DBObject bson) throws XPathException {
//
//        return getValues(context, bson);
//
//    }

//    // BSONObject
//
//    public static Sequence getValues(XQueryContext context, Object o) throws XPathException {
//
//        if(o==null){
//            return Sequence.EMPTY_SEQUENCE;
//        }
//
//        Sequence retVal = new ValueSequence();
//
//        if (o instanceof BasicDBObject) {
//
//            BasicDBObject bson = (BasicDBObject) o;
//            MapType mt = new MapType(context);
//
//            for (Map.Entry<String, Object> entry : bson.entrySet()) {
//
//                String key = entry.getKey();
//                Object value = entry.getValue();
//
//                mt.add(new StringValue(key), getValues(context, value));
//
//            }
//            retVal.add(mt);
//
//        } else if (o instanceof BasicDBList) {
//            BasicDBList list = (BasicDBList) o;
//
//            List<Sequence> collected = new ArrayList<>();
//
//            for (Object item : list) {
//                collected.add(getValues(context, item));
//            }
//
//            ArrayType at = new ArrayType(context, collected);
//            retVal.add(at);
//
//        } else {
//            // regular javaobject
//            // TODO : create actual objects
//            retVal = XPathUtil.javaObjectToXPath(o, context);
//        }
//
//        return retVal;
//
//    }

    /**
     * Convert Sequence into array of Java objects
     */
    public static Object[] convertParameters(final Sequence args) throws XPathException {
        final List<Object> params = new ArrayList<>();
        final SequenceIterator iterate = args.iterate();
        while (iterate.hasNext()) {

            final Item item = iterate.nextItem();

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
