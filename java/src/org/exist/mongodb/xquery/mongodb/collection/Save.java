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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to save a document in mongodb
 *
 * @author Dannes Wessels
 */
public class Save extends BasicFunction {

    private static final String SAVE = "save";
    
        public static final String PARAM_JSONCONTENT = "content";
    public static final String DESCR_JSONCONTENT = "Document content as JSON formatted document";

    public static final FunctionParameterSequenceType PARAMETER_JSONCONTENT
            = new FunctionParameterSequenceType(PARAM_JSONCONTENT, Type.ITEM, Cardinality.ONE, DESCR_JSONCONTENT);
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(SAVE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Update an existing document or insert a document depending on the parameter. "
                + "If the document does not contain an '_id' field, then the method performs an insert with the specified fields in the document as well "
                + "as an '_id' field with a unique objectId value. If the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field: " +
"If a document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document. " +
"If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record with the fields from the document.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_JSONCONTENT},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The write result, JSON formatted")
        ),};

    public Save(XQueryContext context, FunctionSignature signature) {
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
            
            // Get data
            BasicDBObject data = ConversionTools.convertJSonParameter(args[3]);
    
            // Execute save
            WriteResult result = dbcol.save(data);

            return new StringValue(result.toString());
            
        } catch (MongoCommandException ex){
            // TODO return as value?
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0005, ex.getMessage());

        } catch (JSONParseException | IllegalArgumentException ex) {
            LOG.error(ex.getMessage());
            throw new XPathException(this, MongodbModule.MONGO_JSON, ex.getMessage());

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
