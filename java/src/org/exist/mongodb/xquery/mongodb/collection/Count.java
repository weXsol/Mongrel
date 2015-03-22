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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_QUERY;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * Count the number of documents in the collection
 *
 * @author Dannes Wessels
 */
public class Count extends BasicFunction {

    private static final String COUNT = "count";
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(COUNT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Count the number of documents in the collection.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION},
        new FunctionReturnSequenceType(Type.LONG, Cardinality.ONE, "Number of documents")
        ),
        
        new FunctionSignature(
        new QName(COUNT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Count the number of documents in the collection that match the query.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_QUERY},
        new FunctionReturnSequenceType(Type.INTEGER, Cardinality.ONE, "Number of documents")
        ),
    };

    public Count(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            // Verify clientid and get client
            String mongodbClientId = args[0].itemAt(0).getStringValue();                  
            MongodbClientStore.getInstance().validate(mongodbClientId);
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);
                        
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();

            // Get query when available
            BasicDBObject mongoQuery = (args.length == 4)
                    ? ConversionTools.convertJSonParameter(args[3])
                    : null;

            // Get database and collection
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);

            // Count documents
            Long nrOfDocuments = (mongoQuery==null) ? dbcol.count() : dbcol.count(mongoQuery);

            return new IntegerValue(nrOfDocuments);

        } catch (JSONParseException ex) {
            String msg = "Invalid JSON data: " + ex.getMessage();
            LOG.error(msg);
            throw new XPathException(this, MongodbModule.MONG0004, msg);

        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0002, ex.getMessage());

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
            throw new XPathException(this, MongodbModule.MONG0003, t.getMessage());
        }

    }


}
