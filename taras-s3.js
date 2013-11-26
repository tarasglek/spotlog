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
 * applies function to every file in bucket returned by an s3 list operation
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
module.exports = {
  S3ListObjects: S3ListObjects, 
  S3GetObjectGunzip: S3GetObjectGunzip,
  S3MapBucket: S3MapBucket
};
