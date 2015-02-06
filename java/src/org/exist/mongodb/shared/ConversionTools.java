package org.exist.mongodb.shared;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.util.ArrayList;
import java.util.List;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;

/**
 *
 * @author wessels
 */
public class ConversionTools {

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
     *  Convert first item in sequence to BSON object
     * @param seq The sequence
     * @return the converted result, null if input is null.
     * @throws JSONParseException The string could not be parsed.
     * @throws XPathException The string value could not be obtained
     */
    public static BasicDBObject convertJSon(Sequence seq) throws JSONParseException, XPathException {
        String json = seq.itemAt(0).getStringValue();
        return convertJSon(json);
    }

}
