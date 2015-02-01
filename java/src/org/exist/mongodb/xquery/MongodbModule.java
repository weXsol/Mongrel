
package org.exist.mongodb.xquery;

import java.util.List;
import java.util.Map;
import org.exist.dom.QName;
import org.exist.mongodb.xquery.gridfs.Remove;
import org.exist.mongodb.xquery.mongodb.client.Close;
import org.exist.mongodb.xquery.mongodb.client.Connect;
import org.exist.mongodb.xquery.mongodb.client.ListDatabases;
import org.exist.mongodb.xquery.mongodb.client.ListMongdbClientIds;
import org.exist.mongodb.xquery.mongodb.collection.Aggregate;
import org.exist.mongodb.xquery.mongodb.collection.Count;
import org.exist.mongodb.xquery.mongodb.collection.Drop;
import org.exist.mongodb.xquery.mongodb.collection.Find;
import org.exist.mongodb.xquery.mongodb.collection.FindAndModify;
import org.exist.mongodb.xquery.mongodb.collection.FindAndRemove;
import org.exist.mongodb.xquery.mongodb.collection.FindOne;
import org.exist.mongodb.xquery.mongodb.collection.Group;
import org.exist.mongodb.xquery.mongodb.collection.Insert;
import org.exist.mongodb.xquery.mongodb.collection.MapReduce;
import org.exist.mongodb.xquery.mongodb.collection.Save;
import org.exist.mongodb.xquery.mongodb.collection.Update;
import org.exist.mongodb.xquery.mongodb.db.EvalCommand;
import org.exist.mongodb.xquery.mongodb.db.ListCollections;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;



public class MongodbModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/mongodb";
    public final static String PREFIX = "mongodb";
    public final static String INCLUSION_DATE = "2014-08-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";

    public final static FunctionDef[] functions = { 
        new FunctionDef(Aggregate.signatures[0], Aggregate.class), 
        new FunctionDef(Close.signatures[0], Close.class),
        new FunctionDef(Connect.signatures[0], Connect.class),
        new FunctionDef(Count.signatures[0], Count.class),
        new FunctionDef(Count.signatures[1], Count.class),   
        new FunctionDef(Drop.signatures[0], Drop.class),
        new FunctionDef(EvalCommand.signatures[0], EvalCommand.class),
        new FunctionDef(EvalCommand.signatures[1], EvalCommand.class),
        new FunctionDef(EvalCommand.signatures[2], EvalCommand.class),
        new FunctionDef(Find.signatures[0], Find.class),
        new FunctionDef(Find.signatures[1], Find.class),
        new FunctionDef(Find.signatures[2], Find.class),
        new FunctionDef(FindAndModify.signatures[0], FindAndModify.class),
        new FunctionDef(FindAndModify.signatures[1], FindAndModify.class),
        new FunctionDef(FindAndRemove.signatures[0], FindAndRemove.class),
        new FunctionDef(FindOne.signatures[0], FindOne.class),
        new FunctionDef(FindOne.signatures[1], FindOne.class),
        new FunctionDef(FindOne.signatures[2], FindOne.class),
        new FunctionDef(FindOne.signatures[3], FindOne.class),
        new FunctionDef(Group.signatures[0], Group.class), 
        new FunctionDef(Insert.signatures[0], Insert.class),
        new FunctionDef(ListCollections.signatures[0], ListCollections.class),
        new FunctionDef(ListDatabases.signatures[0], ListDatabases.class),
        new FunctionDef(ListMongdbClientIds.signatures[0], ListMongdbClientIds.class),   
        new FunctionDef(MapReduce.signatures[0], MapReduce.class),
        new FunctionDef(Remove.signatures[0], Remove.class), 
        new FunctionDef(Save.signatures[0], Save.class),
        new FunctionDef(Update.signatures[0], Update.class),
    };
    
    public final static ErrorCode MONG0001 = new MongodbErrorCode("MONG0001", "Forbidden");
    public final static ErrorCode MONG0002 = new MongodbErrorCode("MONG0002", "Mongodb exception");
    public final static ErrorCode MONG0003 = new MongodbErrorCode("MONG0003", "Generic exception");
    public final static ErrorCode MONG0004 = new MongodbErrorCode("MONG0004", "JSON Syntax exception");
    public final static ErrorCode MONG0005 = new MongodbErrorCode("MONG0005", "Command exception");

    public final static QName EXCEPTION_QNAME
            = new QName("exception", MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", MongodbModule.NAMESPACE_URI, MongodbModule.PREFIX);

    public MongodbModule(Map<String, List<? extends Object>> parameters) throws XPathException {
        super(functions, parameters);
    }

    @Override
    public String getNamespaceURI() {
        return NAMESPACE_URI;
    }

    @Override
    public String getDefaultPrefix() {
        return PREFIX;
    }

    @Override
    public String getDescription() {
        return "MongoDB module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
    
    protected final static class MongodbErrorCode extends ErrorCode {

        public MongodbErrorCode(String code, String description) {
            super(new QName(code, NAMESPACE_URI, PREFIX), description);
        }

    }
}
