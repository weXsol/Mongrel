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
package org.exist.mongodb.xquery.bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
import org.exist.mongodb.shared.ConversionTools;
import org.exist.mongodb.xquery.BSonModule;
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
 * Functions to Parse JSON/BSON.
 *
 * @author Dannes Wessels
 */
public class Parse extends BasicFunction {

    private static final String PARSE_AS_STRING = "parse-as-string";
    private static final String PARSE = "parse";
    
    public static final String PARAM_JSONCONTENT = "content";
    public static final String DESCR_JSONCONTENT = "JSON formatted document or item";

    public static final FunctionParameterSequenceType PARAMETER_JSONCONTENT
            = new FunctionParameterSequenceType(PARAM_JSONCONTENT, Type.ITEM, Cardinality.ONE, DESCR_JSONCONTENT);
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName(PARSE_AS_STRING, BSonModule.NAMESPACE_URI, BSonModule.PREFIX), "JSON data tthat needs to be parsed.",
        new SequenceType[]{
            PARAMETER_JSONCONTENT},
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The parse result, JSON formatted")
        ),
        new FunctionSignature(
        new QName(PARSE, BSonModule.NAMESPACE_URI, BSonModule.PREFIX), "JSON data that needs to be parsed.",
        new SequenceType[]{
            PARAMETER_JSONCONTENT},
        new FunctionReturnSequenceType(Type.NODE, Cardinality.ONE, "The parse result")
        ),
    
    };

    public Parse(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {
            BasicDBObject data = ConversionTools.convertJSon(args[0]);

            if(isCalledAs(PARSE)){
                return ConversionTools.convertBson(context, data);
                
            } else {
                return new StringValue(data.toString());
            }
            
        } catch (MongoCommandException ex){
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, BSonModule.MONG0005, ex.getMessage());

        } catch (JSONParseException ex) {
            String msg = "Invalid JSON data: " + ex.getMessage();
            LOG.error(msg);
            throw new XPathException(this, BSonModule.MONG0004, msg);

        } catch (XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, BSonModule.MONG0002, ex.getMessage());

        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
            throw new XPathException(this, BSonModule.MONG0003, t.getMessage());
        }

    }

}
