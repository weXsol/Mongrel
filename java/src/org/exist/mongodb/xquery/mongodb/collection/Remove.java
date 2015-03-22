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
 * Function to Remove document from mongodb
 *
 * @author Dannes Wessels
 */
public class Remove extends BasicFunction {

    private static final String REMOVE = "remove";

    public static final String PARAM_DELETE_CRITERIUM = "criterium";
    public static final String DESCR_DELETE_CRITERIUM = "The deletion criteria using query operators. Omit "
            + "the query parameter or pass an empty document to delete all documents in the collection. JSON formatted";

    public static final FunctionParameterSequenceType PARAMETER_DELETE_CRITERIUM
            = new FunctionParameterSequenceType(PARAM_DELETE_CRITERIUM, Type.ITEM, Cardinality.ONE, DESCR_DELETE_CRITERIUM);
    
  
    public final static FunctionSignature signatures[] = {
        
        new FunctionSignature(
        new QName(REMOVE, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Remove documents from a collection.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_DELETE_CRITERIUM},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The document as it was before it was removed formatted as JSON")
        ),
        
    };

    public Remove(XQueryContext context, FunctionSignature signature) {
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
            
            BasicDBObject query = (args.length >= 4)
                    ? ConversionTools.convertJSonParameter(args[3])
                    : null;

            // Get collection in database
            DB db = client.getDB(dbname);
            DBCollection dbcol = db.getCollection(collection);
            
            // Execute query      
            WriteResult result = dbcol.remove(query);
            
            return new StringValue(result.toString());
            
        } catch (JSONParseException ex) {
            LOG.error(ex.getMessage());
            throw new XPathException(this, MongodbModule.MONGO_JSON, ex.getMessage());
            
        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0002, ex.getMessage());

        } catch (Throwable ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, MongodbModule.MONG0003, ex.getMessage());
        }

    }
    

}
