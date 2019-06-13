/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2015 The eXist Project
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
package org.exist.mongodb.shared;

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.util.JSONParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.mongodb.xquery.MongodbModule;
import org.exist.xquery.Expression;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Sequence;

/**
 * Handle mongodb exceptions in a generic way by translating them into an
 * existdb XpathExeption
 *
 * @author Dannes Wessels
 */
public class GenericExceptionHandler {

    private final static Logger LOG = LogManager.getLogger(GenericExceptionHandler.class);

    /**
     * Process the exception thrown by the Mongodb driver.
     *
     * @param expr      The current xpath expression
     * @param throwable The Exception
     * @return Nothing, there will always be an exception thrown.
     * @throws XPathException The translated eXistdb exception
     */
    public static Sequence handleException(final Expression expr, final Throwable throwable) throws XPathException {

        if (LOG.isDebugEnabled()) {
            LOG.error(throwable.getMessage());
        } else {
            LOG.error(throwable.getMessage(), throwable);
        }

        if (throwable instanceof XPathException) {
            throw (XPathException) throwable;
        } else if (throwable instanceof IllegalArgumentException) {
            throw new XPathException(expr, MongodbModule.MONGO_JSON, throwable.getMessage());

        } else if (throwable instanceof JSONParseException) {
            throw new XPathException(expr, MongodbModule.MONGO_JSON, throwable.getMessage());

        } else if (throwable instanceof MongoTimeoutException) {
            throw new XPathException(expr, MongodbModule.MONG0006, throwable.getMessage());

        } else if (throwable instanceof MongoClientException) {
            throw new XPathException(expr, MongodbModule.MONG0007, throwable.getMessage());

        } else if (throwable instanceof MongoCommandException) {
            throw new XPathException(expr, MongodbModule.MONG0005, throwable.getMessage());

        } else if (throwable instanceof MongoException) {
            throw new XPathException(expr, MongodbModule.MONG0002, throwable);

        } else {
            throw new XPathException(expr, MongodbModule.MONG0003, throwable);
        }

    }

}
