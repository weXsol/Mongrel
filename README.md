Mongrel: the eXist-db MongoDB extension
========================================

The Mongrel eXist-db extension provides xquery extension functions to access MongoDB database functions.

In addition to "regular" [MongoDB](https://github.com/dizzzz/Mongrel/wiki/MongoDB) operations the extension also provides [GridFS](https://github.com/dizzzz/Mongrel/wiki/GridFS) functions to manage arbitrary sized documents that are stored in MongoDB.

Extensive documentation can be found on the [Wiki](https://github.com/dizzzz/Mongrel/wiki).

Downloads and release notes are on the GitHub [Releases](https://github.com/dizzzz/Mongrel/releases) page.

![MongoDB Logo](http://www.mongodb.com/sites/mongodb.com/files/media/mongodb-logo-rgb.jpeg)

## Requirements
- eXist-db 2.2
- Java 8

## Notes
The current released code serializes JSON into `xs:string`. For [XQuery 3.1](http://www.w3.org/TR/xquery-31/) (probably supported by eXist-db 2.3) JSON is part of the specification, using Maps and Arrays. Migration will certainly break the API in the future.

## License

The work is released AS-IS under the LGPL license.
