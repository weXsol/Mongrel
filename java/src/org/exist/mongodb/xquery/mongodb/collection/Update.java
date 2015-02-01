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
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
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
public class Update extends BasicFunction {

    private static final String UPDATE = "update";
    
    public static final String PARAM_CRITERIA = "criteria";
    public static final String DESCR_CRITERIA = "The selection criteria for the update";

    public static final FunctionParameterSequenceType PARAMETER_CRITERIA
            = new FunctionParameterSequenceType(PARAM_CRITERIA, Type.STRING, Cardinality.ONE, DESCR_CRITERIA);
    
    public static final String PARAM_MODIFICATION = "modification";
    public static final String DESCR_MODIFICATION  = "The modifications to apply";

    public static final FunctionParameterSequenceType PARAMETER_MODIFICATION 
            = new FunctionParameterSequenceType(PARAM_MODIFICATION, Type.STRING, Cardinality.ONE, DESCR_MODIFICATION);
    
    public static final String PARAM_UPSERT = "upsert";
    public static final String DESCR_UPSERT  = "When true, inserts a document if no document matches the update query criteria";

    public static final FunctionParameterSequenceType PARAMETER_UPSERT 
            = new FunctionParameterSequenceType(PARAM_UPSERT, Type.BOOLEAN, Cardinality.ONE, DESCR_UPSERT);
    
    public static final String PARAM_MULTI = "multi";
    public static final String DESCR_MULTI  = "When true, updates all documents in the collection that match the update query criteria, otherwise only updates one";

    public static final FunctionParameterSequenceType PARAMETER_MULTI 
            = new FunctionParameterSequenceType(PARAM_MULTI, Type.BOOLEAN, Cardinality.ONE, DESCR_MULTI);
    
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(UPDATE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Modify an existing document or documents in collection. By default the method updates a single document.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_CRITERIA, PARAMETER_MODIFICATION},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The write result, JSON formatted")
        ),
        
        new FunctionSignature(
        new QName(UPDATE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Modify an existing document or documents in collection.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_CRITERIA, PARAMETER_MODIFICATION, PARAMETER_UPSERT, PARAMETER_MULTI},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The write result, JSON formatted")
        ),
    
    };

    public Update(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String collection = args[2].itemAt(0).getStringValue();

            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);
            
            // Get data
            BasicDBObject criterium = (BasicDBObject) JSON.parse(args[3].itemAt(0).getStringValue());
            BasicDBObject modification = (BasicDBObject) JSON.parse(args[4].itemAt(0).getStringValue());
            
            Boolean upsert = (args.length >= 6)
                    ? args[5].itemAt(0).toJavaObject(Boolean.class)
                    : null;
            
            Boolean multi = (args.length >= 7)
                    ? args[6].itemAt(0).toJavaObject(Boolean.class)
                    : null;
    
            // Execute update
            WriteResult update = (upsert == null) 
                    ? dbcol.update(criterium, modification)
                    : dbcol.update(criterium, modification, upsert, multi);

            return new StringValue(update.toString());
            
        } catch (MongoCommandException ex){
            // TODO return as value?
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0005, ex.getMessage());

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
