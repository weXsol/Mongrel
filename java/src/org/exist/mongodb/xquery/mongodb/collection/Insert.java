/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.mongodb.xquery.mongodb.collection;

import com.mongodb.*;
import org.apache.commons.lang3.StringUtils;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;

import java.util.ArrayList;
import java.util.List;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Insert extends BasicFunction {

    public static final String PARAM_JSONCONTENT = "content";
    public static final String DESCR_JSONCONTENT = "Document content as JSON formatted document";
    public static final FunctionParameterSequenceType PARAMETER_JSONCONTENT
            = new FunctionParameterSequenceType(PARAM_JSONCONTENT, Type.ITEM, Cardinality.ZERO_OR_MORE, DESCR_JSONCONTENT);
    private static final String INSERT = "insert";
    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName(INSERT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Insert data",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_JSONCONTENT},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The insert result")
            ),};

    public Insert(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get parameters
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            // Place holder for all results
            List<DBObject> allContent = new ArrayList<>();

            SequenceIterator iterate = args[3].iterate();
            while (iterate.hasNext()) {

                Item nextItem = iterate.nextItem();

                if (nextItem instanceof Sequence) { // Dead code
                    Sequence seq = (Sequence) nextItem;
                    BasicDBObject bsonContent = ConversionTools.convertJSonParameter(seq);
                    allContent.add(bsonContent);

                } else {
                    String value = iterate.nextItem().getStringValue();
                    if (StringUtils.isEmpty(value)) {
                        LOG.error("Skipping empty string");
                    } else {
                        DBObject bsonContent = ConversionTools.convertJSon(value);
                        allContent.add(bsonContent);
                    }
                }
            }

            WriteResult result = dbcol.insert(allContent);


            // Wrap results into map
            final MapType map = new MapType(context);
            map.add(new StringValue("acknowledged"), new ValueSequence(new BooleanValue(result.wasAcknowledged())));

            if (result.wasAcknowledged()) {
                map.add(new StringValue("n"), new ValueSequence(new IntegerValue(result.getN())));
                map.add(new StringValue("updateOfExisting"), new ValueSequence(new BooleanValue(result.isUpdateOfExisting())));
                map.add(new StringValue("upsertedId"), new ValueSequence(new StringValue((String) result.getUpsertedId())));
            }

            return map;

        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }

}
