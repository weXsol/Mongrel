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
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;

import static org.exist.mongodb.shared.FunctionDefinitions.*;

/**
 * Functions to save a document in mongodb
 *
 * @author Dannes Wessels
 */
public class Update extends BasicFunction {

    public static final String PARAM_CRITERIA = "criteria";
    public static final String DESCR_CRITERIA = "The selection criteria for the update";
    public static final FunctionParameterSequenceType PARAMETER_CRITERIA
            = new FunctionParameterSequenceType(PARAM_CRITERIA, Type.ITEM, Cardinality.ONE, DESCR_CRITERIA);
    public static final String PARAM_MODIFICATION = "modification";
    public static final String DESCR_MODIFICATION = "The modifications to apply";
    public static final FunctionParameterSequenceType PARAMETER_MODIFICATION
            = new FunctionParameterSequenceType(PARAM_MODIFICATION, Type.ITEM, Cardinality.ONE, DESCR_MODIFICATION);
    public static final String PARAM_UPSERT = "upsert";
    public static final String DESCR_UPSERT = "When true, inserts a document if no document matches the update query criteria";
    public static final FunctionParameterSequenceType PARAMETER_UPSERT
            = new FunctionParameterSequenceType(PARAM_UPSERT, Type.BOOLEAN, Cardinality.ONE, DESCR_UPSERT);
    public static final String PARAM_MULTI = "multi";
    public static final String DESCR_MULTI = "When true, updates all documents in the collection that match the update query criteria, otherwise only updates one";
    public static final FunctionParameterSequenceType PARAMETER_MULTI
            = new FunctionParameterSequenceType(PARAM_MULTI, Type.BOOLEAN, Cardinality.ONE, DESCR_MULTI);
    private static final String UPDATE = "update";
    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName(UPDATE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Modify an existing document or documents in collection. By default the method updates a single document.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_CRITERIA, PARAMETER_MODIFICATION},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The update result")
            ),

            new FunctionSignature(
                    new QName(UPDATE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Modify an existing document or documents in collection.",
                    new SequenceType[]{
                            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_CRITERIA, PARAMETER_MODIFICATION, PARAMETER_UPSERT, PARAMETER_MULTI},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The update result")
            ),

    };

    public Update(XQueryContext context, FunctionSignature signature) {
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



            // Get data
            BasicDBObject criterium = ConversionTools.convertJSonParameter(args[3]);
            BasicDBObject modification = ConversionTools.convertJSonParameter(args[4]);

            Boolean upsert = (args.length >= 6)
                    ? args[5].itemAt(0).toJavaObject(Boolean.class)
                    : null;

            Boolean multi = (args.length >= 7)
                    ? args[6].itemAt(0).toJavaObject(Boolean.class)
                    : null;

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            // Execute update
            WriteResult result = (upsert == null)
                    ? dbcol.update(criterium, modification)
                    : dbcol.update(criterium, modification, upsert, multi);

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
