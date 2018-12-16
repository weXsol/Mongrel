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

import com.mongodb.DBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.util.JSONParseException;
import org.exist.dom.QName;
import org.exist.mongodb.shared.BSONtoMap;
import org.exist.mongodb.shared.MapToBSON;
import org.exist.mongodb.xquery.BSonModule;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * Functions to Parse JSON/BSON.
 *
 * @author Dannes Wessels
 */
public class Parse extends BasicFunction {

    public static final String PARAM_JSONCONTENT = "content";
    public static final String DESCR_JSONCONTENT = "JSON formatted document or item";
    public static final FunctionParameterSequenceType PARAMETER_JSONCONTENT
            = new FunctionParameterSequenceType(PARAM_JSONCONTENT, Type.ITEM, Cardinality.ONE, DESCR_JSONCONTENT);
    private static final String PARSE_AS_STRING = "parse-as-string";
    private static final String PARSE = "parse";
    public final static FunctionSignature[] signatures = {
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

    public Parse(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        try {
            final DBObject data = MapToBSON.convert(args[0]);

            if (isCalledAs(PARSE)) {
                return BSONtoMap.convert(data, context);

            } else {
                return new StringValue(data.toString());
            }

        } catch (final MongoCommandException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, BSonModule.MONG0005, ex.getMessage());

        } catch (final JSONParseException ex) {
            final String msg = "Invalid JSON data: " + ex.getMessage();
            LOG.error(msg);
            throw new XPathException(this, BSonModule.MONG0004, msg);

        } catch (final XPathException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);

        } catch (final MongoException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, BSonModule.MONG0002, ex.getMessage());

        } catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
            throw new XPathException(this, BSonModule.MONG0003, t.getMessage());
        }

    }

}
