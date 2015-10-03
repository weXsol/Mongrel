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
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_COLLECTION;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import org.exist.mongodb.shared.GenericExceptionHandler;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Insert extends BasicFunction {

    private static final String INSERT = "insert";


    public static final String PARAM_JSONCONTENT = "content";
    public static final String DESCR_JSONCONTENT = "Document content as JSON formatted document";

    public static final FunctionParameterSequenceType PARAMETER_JSONCONTENT
            = new FunctionParameterSequenceType(PARAM_JSONCONTENT, Type.ITEM, Cardinality.ZERO_OR_MORE, DESCR_JSONCONTENT);
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(INSERT, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Insert data",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_COLLECTION, PARAMETER_JSONCONTENT},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The write result, JSON formatted")
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
            List<DBObject> allContent = new ArrayList();
            
            SequenceIterator iterate = args[3].iterate();
            while(iterate.hasNext()){

                Item nextItem = iterate.nextItem();

                if(nextItem instanceof Sequence){ // Dead code
                    Sequence seq = (Sequence) nextItem;
                    BasicDBObject bsonContent = ConversionTools.convertJSonParameter(seq);
                    allContent.add(bsonContent);

                } else {
                    String value = iterate.nextItem().getStringValue();
                    if(StringUtils.isEmpty(value)){
                        LOG.error("Skipping empty string");
                    } else {
                        DBObject bsonContent = ConversionTools.convertJSon(value);
                        allContent.add(bsonContent);
                    }
                }
            }
    
            WriteResult result = dbcol.insert(allContent);

            return new StringValue(result.toString());
            
        } catch (Throwable t) {
            return GenericExceptionHandler.handleException(this, t);
        } 

    }

}
