'use strict';
/**
 * @file mongodb-restore main
 * @module mongodb-restore
 * @subpackage main
 * @version 1.6.0
 * @author hex7c0 <hex7c0@gmail.com>
 * @copyright hex7c0 2014
 * @license GPLv3
 */

/*
 * initialize module
 */
var systemRegex = /^system\./;
var fs = require('graceful-fs');
var path = require('path');
var logger;

/*
 * functions
 */
/**
 * error handler
 *
 * @function error
 * @param {Object} err - raised error
 */
function error(err) {

    if (err) {
        logger(err.message);
    }
}

function jsonParser() {
    return {
        parse: JSON.parse
    };
}

function bsonParser() {
    var BSON = require('bson');
    BSON = new BSON();

    return {
        parse: BSON.deserialize
    };
}

var streamListener = function(parser, writer, supportMetadata){
    var collectionNameForDirectory = new RegExp('.*/([^/]+)/?$');
    var collectionNameForFiles = new RegExp('.*/([^/]+)/[^/]+$');
    var metadataFilenameRegexp = new RegExp('.*/[^/]+/([^/]+)$');

    var metadata = [];
    return {
        onDirectory: function (directoryName, callback) {
            // Parse collection name out
            var collectionName = collectionNameForDirectory.exec(directoryName.replace(/\\/g, '/'))[1];
            if (collectionName === '.metadata') {
                callback(null);
                return;
            }
            writer.createCollection(collectionName, callback);
        },

        onFile: function(name, stream, callback) {
            var collectionName = collectionNameForFiles.exec(name.replace(/\\/g, '/'))[1];
            if (collectionName === '.metadata') {
                // Extract metadata collection name
                var metadataName = metadataFilenameRegexp.exec(name.replace(/\\/g, '/'))[1];
                //parse the metadata file

                // Callback and return only if metadata support disabled
                if(!supportMetadata) {
                    callback(null);
                    return;
                }
            }

            var buffers = [];
            stream.on('data',function(buffer){
                buffers.push(buffer);
            });


            stream.on('end',function(){
                // If collectionName ==== '.metadata', store metadata contents in metadata array
                if (collectionName === '.metadata') {
                    metadata.push({
                        collectionName: metadataName,
                        document: jsonParser().parse(Buffer.concat(buffers))
                    });
                    callback(null);
                    return;
                } else {
                    writer.addDocument(collectionName, parser.parse(Buffer.concat(buffers)), callback);
                }
            });
        },

        finish: function(callback) {
            writer.drain(function(err) {
                error(err);
                if(!supportMetadata){
                    callback();
                    return;
                }

                writer.addIndices(metadata, function(err) {
                    error(err);
                    callback();
                    return;
                });

            });
        }
    };
};

var writer = function () {
    return function (db) {
        var dataToWrite = {};

        var writeAllInCollection = function (collectionName, callback) {
            var operations = dataToWrite[collectionName].map(function (document) {
                return ({
                    insertOne: {
                        document: document
                    }
                });
            });

            if (operations.length === 0) {
                callback(null);
                return;
            }
            db.collection(collectionName, function (err, collection) {
                error(err);
                collection.bulkWrite(operations, function(err) {
                    delete dataToWrite[collectionName];
                    callback(err);
                });
            });
        };
        var writer = {
            createCollection: function(collectionName, next) {
                db.createCollection(collectionName, next);
            },

            addDocument: function(collectionName, document, next) {
                if (Array.isArray(dataToWrite[collectionName])) {
                    dataToWrite[collectionName].push(document);
                } else {
                    dataToWrite[collectionName] = [document];
                }
                if (dataToWrite[collectionName].length > 50) {
                    writeAllInCollection(collectionName, function(err) {
                        next(err);
                    });
                } else {
                    next(null);
                }
            },

            drain: function(callback) {
                if (Object.keys(dataToWrite).length === 0) {
                    callback(null);
                    return;
                }

                var collectionName = Object.keys(dataToWrite)[0];
                writeAllInCollection(collectionName, function (callback, err) {
                    error(err);
                    writer.drain(callback);
                }.bind(this, callback));
            },

            addIndices: function(indices, callback) {
                if (indices.length === 0) {
                    callback(null);
                    return;
                }

                var collectionAndIndexes = indices.pop();
                db.command({createIndexes: collectionAndIndexes.collectionName, indexes: collectionAndIndexes.document}, {}, function(err) {
                    error(err);
                    writer.addIndices(indices, callback);
                });
            }
        };

        return writer;
    };
}();

/**
 * drop data from some collections
 *
 * @function someCollections
 * @param {Object} db - database
 * @param {Array} collections - selected collections
 * @param {Function} next - callback
 */
function someCollections(db, collections, next) {

    var last = ~~collections.length, counter = 0;
    if (last === 0) { // empty set
        return next(null);
    }

    collections.forEach(function(collection) {

        db.collection(collection, function(err, collection) {

            logger('select collection ' + collection.collectionName);
            if (err) {
                return last === ++counter ? next(err) : error(err);
            }
            collection.drop(function(err) {

                if (err) {
                    error(err); // log if missing
                }
                return last === ++counter ? next(null) : null;
            });
        });
    });
}

/**
 * function wrapper
 *
 * @function wrapper
 * @param {Object} my - parsed options
 */
function wrapper(my) {

    var parser;
    if (typeof my.parser === 'function') {
        parser = my.parser;
    } else {
        switch (my.parser.toLowerCase()) {
            case 'bson':
                parser = bsonParser();
                break;
            case 'json':
                // JSON error on ObjectId and Date
                parser = jsonParser();
                break;
            default:
                throw new Error('missing parser option');
        }
    }

    if (my.logger === null) {
        logger = function() {

            return;
        };
    } else {
        logger = require('logger-request')({
            filename: my.logger,
            standalone: true,
            daily: true,
            winston: {
                logger: '_mongo_r' + my.logger,
                level: 'info',
                json: false
            }
        });
        logger('restore start');
        var log = require('mongodb').Logger;
        log.setLevel('info');
        log.setCurrentLogger(function(msg) {

            logger(msg);
        });
    }

    /**
     * latest callback
     *
     * @return {Null}
     */
    function callback(err) {

        logger('restore stop');

        if (my.callback !== null) {
            logger('callback run');
            my.callback(err);

        } else if (err) {
            logger(err);
        }
    }

    function tarDataSource(tarFile) {
        return streamingTarDataSource(fs.createReadStream(tarFile));
    }

    function streamingTarDataSource(tarStream) {
        return {
            begin: function(streamListener, callback) {
                var tar = require('tar-stream');

                var extract = tar.extract();

                extract.on('entry', function(header, stream, next) {
                    // header is the tar header
                    if (header.type === 'directory') {
                        streamListener.onDirectory(header.name, next);
                    } else if (header.type === 'file') {
                        streamListener.onFile(header.name, stream, next);
                    } else {
                        next();
                    }

                    stream.resume(); // just auto drain the stream
                });

                extract.on('finish', function() {
                    streamListener.finish(callback);
                });

                tarStream.pipe(extract);
            }
        };
    }

    function fileSystemDataSource(rootDir) {
        var addCollections = function(streamListener, collections, callback) {
            if (collections.length === 0) {
                callback(null);
                return;
            }
            var collectionName = collections.pop();
            streamListener.onDirectory(collectionName, function(err) {error(err); addCollections(streamListener, collections, callback);});
        };
        var addFiles = function(streamListener, files, callback) {
            if (files.length === 0) {
                callback(null);
                return;
            }
            var filename = files.pop();
            streamListener.onFile(filename, fs.createReadStream(filename), function(err) {
                error(err);
                addFiles(streamListener, files, callback);
            });
        };
        return {
            begin: function (streamListener, callback) {
                var dirs = fs.readdirSync(rootDir);
                if (dirs.length !== 1) {
                    callback('Found multiple directories and only support one: ' + JSON.toString(dirs));
                    return;
                }

                var dbRootDir = path.join(rootDir, dirs[0]);
                var collections = fs.readdirSync(dbRootDir).map(function(dir) { return path.join(dbRootDir, dir);});
                addCollections(streamListener, collections, function(err) {
                    error(err);
                    var directoryArrays = fs.readdirSync(dbRootDir).map(function(dirname) {
                        return fs.readdirSync(path.join(dbRootDir, dirname)).map(function(filename) {return path.join(dbRootDir, dirname, filename);});
                    });
                    var allFiles = [].concat.apply([], directoryArrays);
                    addFiles(streamListener, allFiles, function(err) {
                        error(err);
                        streamListener.finish(callback);
                    });
                });
            }
        };
    }

    /**
     * entry point
     *
     * @return {Null}
     */
    function go(datasource) {
        require('mongodb').MongoClient.connect(my.uri, my.options,
            function(err, db) {

                logger('db open');
                if (err) {
                    return callback(err);
                }

                function importDataToDb(err) {
                    if (err) {
                        logger('db close');
                        db.close();
                        return callback(err);
                    }

                    datasource.begin(streamListener(parser, writer(db), my.metadata), function(err) {
                        logger('db close');
                        db.close();
                        callback(err);
                    });
                }

                if (my.drop === true) {
                    logger('drop database');
                    return db.dropDatabase(importDataToDb);

                } else if (my.dropCollections) {
                    logger('drop collections');
                    if (Array.isArray(my.dropCollections) === true) {
                        return someCollections(db, my.dropCollections, importDataToDb);
                    }
                    return db.collections(function(err, collections) {

                        if (err) { // log if missing
                            error(err);
                        }
                        my.dropCollections = [];
                        for (var i = 0, ii = collections.length; i < ii; ++i) {
                            var collectionName = collections[i].collectionName;
                            if (systemRegex.test(collectionName) === false) {
                                my.dropCollections.push(collectionName);
                            }
                        }
                        someCollections(db, my.dropCollections, importDataToDb);
                    });
                }

                importDataToDb(null);
            });
    }

    if (!my.tar) {
        return go(fileSystemDataSource(my.root));
    }

    if (my.stream !== null) {
        go(streamingTarDataSource(my.stream));
    } else {
        go(tarDataSource(my.root + my.tar));
    }
}

/**
 * option setting
 *
 * @exports restore
 * @function restore
 * @param {Object} options - various options. Check README.md
 */
function restore(options) {

    var opt = options || Object.create(null);
    if (!opt.uri) {
        throw new Error('missing uri option');
    }
    if (!opt.stream) {
        if (!opt.root) {
            throw new Error('missing root option');
        } else if (!fs.existsSync(opt.root) || !fs.statSync(opt.root).isDirectory()) {
            throw new Error('root option is not a directory');
        }
    }

    var my = {
        dir: path.join(__dirname, 'dump', path.sep),
        uri: String(opt.uri),
        root: path.resolve(String(opt.root)) + path.sep,
        stream: opt.stream || null,
        parser: opt.parser || 'bson',
        callback: typeof opt.callback === 'function' ? opt.callback : null,
        tar: typeof opt.tar === 'string' ? opt.tar : null,
        logger: typeof opt.logger === 'string' ? path.resolve(opt.logger) : null,
        metadata: Boolean(opt.metadata),
        drop: Boolean(opt.drop),
        dropCollections: Boolean(opt.dropCollections) ? opt.dropCollections : null,
        options: typeof opt.options === 'object' ? opt.options : {}
    };
    if (my.stream) {
        my.tar = true; // override
    }
    wrapper(my);
}
module.exports = restore;


