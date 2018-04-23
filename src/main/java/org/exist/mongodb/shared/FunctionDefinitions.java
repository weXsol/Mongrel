package org.exist.mongodb.shared;

import org.exist.xquery.Cardinality;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.Type;

/**
 * @author wessels
 */
public class FunctionDefinitions {

    public static final String PARAM_MONGODB_CLIENT_ID = "mongodbClientId";
    public static final String DESCR_MONGODB_CLIENT_ID = "MongoDB client id";

    public static final FunctionParameterSequenceType PARAMETER_MONGODB_CLIENT
            = new FunctionParameterSequenceType(PARAM_MONGODB_CLIENT_ID, Type.STRING, Cardinality.ONE, DESCR_MONGODB_CLIENT_ID);

    public static final String PARAM_DATABASE = "database";
    public static final String DESCR_DATABASE = "Name of database";

    public static final FunctionParameterSequenceType PARAMETER_DATABASE
            = new FunctionParameterSequenceType(PARAM_DATABASE, Type.STRING, Cardinality.ONE, DESCR_DATABASE);

    public static final String PARAM_BUCKET = "bucket";
    public static final String DESCR_BUCKET = "Name of bucket";

    public static final FunctionParameterSequenceType PARAMETER_BUCKET
            = new FunctionParameterSequenceType(PARAM_BUCKET, Type.STRING, Cardinality.ONE, DESCR_BUCKET);

    public static final String PARAM_COLLECTION = "collection";
    public static final String DESCR_COLLECTION = "Name of collection";

    public static final FunctionParameterSequenceType PARAMETER_COLLECTION
            = new FunctionParameterSequenceType(PARAM_COLLECTION, Type.STRING, Cardinality.ONE, DESCR_COLLECTION);


    public static final String PARAM_OBJECT_ID = "objectid";
    public static final String DESCR_OBJECT_ID = "ObjectID of document";

    public static final FunctionParameterSequenceType PARAMETER_OBJECTID
            = new FunctionParameterSequenceType(PARAM_OBJECT_ID, Type.STRING, Cardinality.ONE, DESCR_OBJECT_ID);

    public static final String PARAM_FILENAME = "filename";
    public static final String DESCR_FILENAME = "Filename of document";

    public static final FunctionParameterSequenceType PARAMETER_FILENAME
            = new FunctionParameterSequenceType(PARAM_FILENAME, Type.STRING, Cardinality.ONE, DESCR_FILENAME);

    public static final String PARAM_CONTENT = "content";
    public static final String DESCR_CONTENT = "Document content as node() or  base64-binary";

    public static final FunctionParameterSequenceType PARAMETER_CONTENT
            = new FunctionParameterSequenceType(PARAM_CONTENT, Type.ITEM, Cardinality.ONE, DESCR_CONTENT);

    public static final String PARAM_CONTENT_TYPE = "contentType";
    public static final String DESCR_CONTENT_TYPE = "Document Content type, use () for mime-type based on file extension";

    public static final FunctionParameterSequenceType PARAMETER_CONTENT_TYPE
            = new FunctionParameterSequenceType(PARAM_CONTENT_TYPE, Type.STRING, Cardinality.ZERO_OR_ONE, DESCR_CONTENT_TYPE);

    public static final String PARAM_AS_ATTACHMENT = "as-attachment";
    public static final String DESCR_AS_ATTACHMENT = "Add content-disposition header";

    public static final FunctionParameterSequenceType PARAMETER_AS_ATTACHMENT
            = new FunctionParameterSequenceType(PARAM_AS_ATTACHMENT, Type.BOOLEAN, Cardinality.ONE, DESCR_AS_ATTACHMENT);

    public static final String PARAM_QUERY = "query";
    public static final String DESCR_QUERY = "The mongodb query, JSON formatted";

    public static final FunctionParameterSequenceType PARAMETER_QUERY
            = new FunctionParameterSequenceType(PARAM_QUERY, Type.MAP, Cardinality.ONE, DESCR_QUERY);


    //the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in the collection.


    public static final String PARAM_KEYS = "options";
    public static final String DESCR_KEYS = "The filters keys, JSON formatted";

    public static final FunctionParameterSequenceType PARAMETER_OPTIONS
            = new FunctionParameterSequenceType(PARAM_KEYS, Type.MAP, Cardinality.ONE, DESCR_KEYS);

    public static final String PARAM_FIELDS = "fields";
    public static final String DESCR_FIELDS = "Fields to return, JSON formatted";

    public static final FunctionParameterSequenceType PARAMETER_FIELDS
            = new FunctionParameterSequenceType(PARAM_FIELDS, Type.ITEM, Cardinality.ONE, DESCR_FIELDS);

    public static final String PARAM_ORDERBY = "orderBy";
    public static final String DESCR_ORDERBY = "Fields to return, JSON formatted";

    public static final FunctionParameterSequenceType PARAMETER_ORDERBY
            = new FunctionParameterSequenceType(PARAM_ORDERBY, Type.ITEM, Cardinality.ONE, DESCR_ORDERBY);

}
