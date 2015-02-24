Mongrel: the eXist-db MongoDB extension
========================================

The Mongrel eXist-db extension provides xquery extension functions to access MongoDB database functions.

In addition to "regular" [MongoDB](https://github.com/dizzzz/Mongrel/wiki/MongoDB) operations the extension also provides [GridFS](https://github.com/dizzzz/Mongrel/wiki/GridFS) functions to manage arbitrary sized documents that are stored in MongoDB.

Extensive documentation can be found on the [Wiki](https://github.com/dizzzz/Mongrel/wiki).

Downloads and release notes are on the GitHub [Releases](https://github.com/dizzzz/Mongrel/releases) page.

![MongoDB Logo](http://www.mongodb.com/sites/mongodb.com/files/media/mongodb-logo-rgb.jpeg)

## Versions

Due to changes in the eXist-db core (restructoring and xquery3.1 support) two versions of the extension are developed: 

- For eXist-2.2 : [Master branch](https://github.com/dizzzz/Mongrel/tree/master)
- For eXist-2.3+ : [Develop branch](https://github.com/dizzzz/Mongrel/tree/develop)

## Requirements
- eXist-db 2.2 / Java7
- eXist-db 2.3+ / Java8

## Notes
The current released code serializes JSON (output) into a JSON `xs:string`. For [XQuery 3.1](http://www.w3.org/TR/xquery-31/) (supported by eXist-db 2.3) JSON is part of the XQuery specification, using Maps and Arrays. Migration to the new output will certainly break the API in the future.

## License
The work is released AS-IS under the LGPL license.

## Support
Support on this extension is provided by [eXist Solutions GmbH](http://www.existsolutions.com)
