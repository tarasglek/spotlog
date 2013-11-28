const AWS = require('aws-sdk');
const async = require('async');
const zlib = require('zlib');

function combineOptions(a, b) {
  var ret = {}
  for (var i in a)
    ret[i] = a[i]
  for (var i in b)
    ret[i] = b[i]
  return ret;
}

function S3ListObjects(s3, options, callback) {
  var retls = null;
  function lister(err, data) {
    if (err)
      return callback(err);

    if (!retls)
      retls = data.Contents;
    else
      retls = retls.concat(data.Contents)

    if (data.IsTruncated) {
      var lastKey = data.Contents[data.Contents.length - 1].Key
      //console.log(retls.length, lastKey);
      s3.listObjects(combineOptions(options, {'Marker':lastKey}), lister)
    } else {
      callback(null, retls);
    }
  }
  
  var req = s3.listObjects(options, lister)
}

var S3GetObjectGunzip = async.compose(function (data, callback) {
                                  zlib.gunzip(data.Body, callback);
                                },
                                function (params, callback) {
                                  params.s3.getObject(params.params, callback);
                                });

/**
 * applies function to every ungzipped file in bucket returned by an s3 list operation
 * mapper - function(fileName, fileContents, callback) that is applied...must call callback with error or result
 */
function S3MapBucket(s3, s3params, limit, mapper, callback) {
  async.waterfall([ function (callback) {
                      S3ListObjects(s3, s3params,
                                    function(err, ls) {
                                      var files = ls.filter(function (x) {return x.Size > 0})
                                        .map(function (x) {return x.Key})
                                      callback(null, files)
                                    });
                    },
                    function (files, callback) {
                      async.mapLimit(files, limit, 
                                     function (file, callback) {
                                       S3GetObjectGunzip({'s3':s3, 'params':{'Bucket':s3params.Bucket, 'Key':file}}, 
                                                         function (err, fileData) {
                                                           if(err)
                                                             return callback(err);
                                                           mapper(file, fileData, callback)
                                                         })
                                     },
                                     function (err, data) {
                                       callback(err, data);
                                     })
                      processedLogs = files
                    }
                  ], callback)
}

function chunkArray(array, n ) {
    if ( !array.length ) {
        return [];
    }
    return [ array.slice( 0, n ) ].concat( chunkArray(array.slice(n), n) );
}

/**
 * performs an s3 copy followed by a delete once all of the copies succeed
 */
function S3Move(s3, s3params, keys, limit, copy_tranformer, delete_transformer, callback) {
  async.waterfall([
                    function(callback) {
                      async.mapLimit(keys, limit, 
                                     function (key, callback) {
                                       s3.copyObject(combineOptions(s3params, copy_tranformer(key)), callback);
                                     },
                                     callback)
                    },
                    function(ignore, callback) {
                      var chunkedBy1000 = chunkArray(keys, 1000);
                      async.mapLimit(chunkedBy1000, limit,
                                     function (keys, callback) {
                                       var keys = keys.map(delete_transformer)
                                       s3.deleteObjects(combineOptions(s3params, {'Delete':{'Objects':keys}}),
                                                        function (err, data) {
                                                          if (err)
                                                            return callback(err);
                                                          if (data.Errors && data.Errors.length)
                                                            return callback(data.Errors)
                                                          callback(null, data);
                                                        });
                                     }, callback);
                    }
                  ], callback);
}

function S3GzipPutObject(s3, s3params, body, callback) {
  var put = async.compose(function (zdata, callback) {
                            s3.putObject(combineOptions(s3params, {'Body': zdata}), callback)    
                          },
                          zlib.gzip
                         );
  put(body, callback);
}

module.exports = {
  S3ListObjects: S3ListObjects, 
  S3GetObjectGunzip: S3GetObjectGunzip,
  S3GzipPutObject: S3GzipPutObject,
  S3MapBucket: S3MapBucket,
  S3Move: S3Move
};
