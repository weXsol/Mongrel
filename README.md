Mongrel: the eXist-db MongoDB extension
========================================

The Mongrel eXist-db extension provides xquery extension functions to access MongoDB database functions.

In addition to "regular" [MongoDB](https://github.com/dizzzz/Mongrel/wiki/MongoDB) operations the extension also provides [GridFS](https://github.com/dizzzz/Mongrel/wiki/GridFS) functions to manage arbitrary sized documents that are stored in MongoDB.

Extensive documentation can be found on the [Wiki](https://github.com/dizzzz/Mongrel/wiki). Downloads and release notes are on the GitHub [Releases](https://github.com/dizzzz/Mongrel/releases) page.

![MongoDB Logo](http://s3.amazonaws.com/info-mongodb-com/_com_assets/media/mongodb-logo-rgb.jpeg)

## Versions

Version 0.6.1 requires eXist-db 3.0 (or newer). 
> The function namespaces are http://expath.org/ns/mongo and http://expath.org/ns/mongo/gridfs

Version 0.3.5 is the last version compatible with eXist-db 2.2.
> The function namespaces were http://exist-db.org/mongrel/mongodb and http://expath.org/ns/mongo/gridfs

## Requirements
- eXist-db 2.2 / Java7
- eXist-db 3.0+ / Java8

## Notes
The version 0.6.1 supports [XQuery 3.1](http://www.w3.org/TR/xquery-31/)  JSON structures and is therefore not XQuery compatible with code written for version 0.3.5 and older.

> the implementation is Work in Progress : there are some specification issues to be solved.

The extension is based on the 2.x version of the [official mongodb driver](http://mongodb.github.io/mongo-java-driver/)

## License
The work is released AS-IS under the LGPL license.

## Support
Support on this extension is provided by [eXist Solutions GmbH](http://www.existsolutions.com)
