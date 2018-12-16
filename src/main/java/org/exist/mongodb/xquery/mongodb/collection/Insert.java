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
import org.exist.mongodb.shared.MapToBSON;
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
    public final static FunctionSignature[] signatures = {
            new FunctionSignature(
                    new QName(INSERT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Insert data",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_JSONCONTENT},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The insert result")
            ),};

    public Insert(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            final String mongodbClientId = args[0].itemAt(0).getStringValue();
            MongodbClientStore.getInstance().validate(mongodbClientId);
            final MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get parameters
            final String dbname = args[1].itemAt(0).getStringValue();
            final String collection = args[2].itemAt(0).getStringValue();

            // Get database
            final DB db = client.getDB(dbname);
            final DBCollection dbcol = db.getCollection(collection);

            // Place holder for all results
            final List<DBObject> allContent = new ArrayList<>();


            final SequenceIterator iterate = args[3].iterate();
            while (iterate.hasNext()) {

                final Item nextItem = iterate.nextItem();


                if (nextItem instanceof MapType) {
                    final Sequence seq = (Sequence) nextItem;
                    final DBObject bsonContent = MapToBSON.convert(seq);
                    allContent.add(bsonContent);

                } else {
                    final String value = nextItem.getStringValue();
                    if (StringUtils.isEmpty(value)) {
                        LOG.error("Skipping empty string");
                    } else {
                        final DBObject bsonContent = ConversionTools.convertJSon(value);
                        allContent.add(bsonContent);
                    }
                }
            }

            final WriteResult result = dbcol.insert(allContent);


            // Wrap results into map
            final MapType map = new MapType(context);
            map.add(new StringValue("acknowledged"), new ValueSequence(new BooleanValue(result.wasAcknowledged())));

            if (result.wasAcknowledged()) {
                map.add(new StringValue("n"), new ValueSequence(new IntegerValue(result.getN())));
                map.add(new StringValue("updateOfExisting"), new ValueSequence(new BooleanValue(result.isUpdateOfExisting())));
                map.add(new StringValue("upsertedId"), new ValueSequence(new StringValue((String) result.getUpsertedId())));
            }

            return map;

        } catch (final Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        }

    }

}
