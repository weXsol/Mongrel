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
package org.exist.mongodb.xquery.gridfs;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.exist.dom.QName;
import org.exist.mongodb.shared.Constants;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.GridfsModule;
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
import org.exist.xquery.value.ValueSequence;

/**
 * Function to list all GridFS buckets
 *
 * @author Dannes Wessels
 */
public class ListBuckets extends BasicFunction {

    private static final String LIST_DOCUMENTS = "list-buckets";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(LIST_DOCUMENTS, GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX),
        "List buckets",
        new SequenceType[]{
            new FunctionParameterSequenceType("mongodbClientId", Type.STRING, Cardinality.ONE, "MongoDB client id"),
            new FunctionParameterSequenceType("database", Type.STRING, Cardinality.ONE, "database"),},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "n/a")
        ),};

    public ListBuckets(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

//        // User must either be DBA or in the JMS group
//        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.MONGODB_GROUP)) {
//            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
//                    context.getSubject().getName(), Constants.MONGODB_GROUP);
//            LOG.error(txt);
//            throw new XPathException(this, txt);
//        }

        try {
            // Ftech parameters
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
                  
            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Retrieve Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Retrieve database          
            DB db = client.getDB(dbname);

            // Retrieve collection names
            Set<String> collectionNames = db.getCollectionNames();
            
            // Storage for results
            ValueSequence valueSequence = new ValueSequence();
           
            // Iterate over collection names ; only pairs of collections
            // with names ending .chunks and .files are buckets
            for (String collName : collectionNames) {
                if (collName.endsWith(".chunks")) {
                    String bucketName = StringUtils.removeEnd(collName, ".chunks");
                    if (collectionNames.contains(bucketName + ".files")) {
                        valueSequence.add(new StringValue(bucketName));
                    }
                }
            }

            return valueSequence;

        } catch (XPathException ex) {
            LOG.error(ex);
            throw ex;

        } catch (MongoException ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);

        } catch (Throwable ex) {
            LOG.error(ex);
            throw new XPathException(this, ex);
        }

        //return Sequence.EMPTY_SEQUENCE;
    }

}
