
package org.exist.mongodb.xquery;

import java.util.List;
import java.util.Map;
import org.exist.dom.QName;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

/**
 *
 * @author wessels
 */


public class GridfsModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/gridfs";
    public final static String PREFIX = "jms";
    public final static String INCLUSION_DATE = "2013-11-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";

    public final static FunctionDef[] functions = { //        new FunctionDef(SendMessage.signatures[0], SendMessage.class),
    //        new FunctionDef(RegisterReceiver.signatures[0], RegisterReceiver.class),
    //        
    //        new FunctionDef(ListReceivers.signatures[0], ListReceivers.class),
    //
    //        new FunctionDef(ManageReceivers.signatures[0], ManageReceivers.class),
    //        new FunctionDef(ManageReceivers.signatures[1], ManageReceivers.class),
    //        new FunctionDef(ManageReceivers.signatures[2], ManageReceivers.class),
    //        new FunctionDef(ManageReceivers.signatures[3], ManageReceivers.class),
    };

    public final static QName EXCEPTION_QNAME
            = new QName("exception", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", GridfsModule.NAMESPACE_URI, GridfsModule.PREFIX);

    public GridfsModule(Map<String, List<? extends Object>> parameters) throws XPathException {
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
        return "GridFS module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
}
