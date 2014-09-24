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
package org.exist.mongodb.xquery.mongodb;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.exist.dom.QName;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_DATABASE;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_JS_QUERY;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_MONGODB_CLIENT;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAMETER_QUERY;
import static org.exist.mongodb.shared.FunctionDefinitions.PARAM_QUERY;
import org.exist.mongodb.shared.MongodbClientStore;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 * Functions to retrieve documents from GridFS as a stream.
 *
 * @author Dannes Wessels
 */
public class Eval extends BasicFunction {

    private static final String EVAL = "eval";
    private static final String COMMAND = "command";

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(EVAL, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Evaluates JavaScript "
                + "functions on the database "
                + "server. This is useful if you need to touch a lot of data lightly, "
                + "in which case network transfer could be a bottleneck",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_JS_QUERY,},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The result")
        ),
        new FunctionSignature(
        new QName(COMMAND, MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX), "Executes a database command.",
        new SequenceType[]{
            PARAMETER_MONGODB_CLIENT, PARAMETER_DATABASE, PARAMETER_QUERY},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The result")
        ),
    };

    public Eval(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            String mongodbClientId = args[0].itemAt(0).getStringValue();
            String dbname = args[1].itemAt(0).getStringValue();
            String query = args[2].itemAt(0).getStringValue();

            // Check id
            MongodbClientStore.getInstance().validate(mongodbClientId);

            // Get Mongodb client
            MongoClient client = MongodbClientStore.getInstance().get(mongodbClientId);

            // Get database
            DB db = client.getDB(dbname);
            
            Sequence retVal;

            if(isCalledAs(EVAL)){
                Object result = db.eval(query);
                retVal = new StringValue(result.toString());
            } else {
                CommandResult result = db.command(query);
                retVal = new StringValue(result.toString());
            }

            return retVal;

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
