const fs = require('fs');
const zlib = require('zlib');
const async = require('async');
const AWS = require('aws-sdk');

function logItem(item, config, retmap) {
  switch(item.eventName) {
    case "TerminateInstances":
    case "RunInstances":
    var rs = item.responseElements
    if (rs) {
      var iset = rs.instancesSet
      iset.items.forEach(function (x) {
                           // x
                           var time = new Date(item.eventTime).getTime();
                           var out = null;
                           if (item.eventName == "RunInstances") {
                             var ret = {}
                             ret.runTime = time
                             ret.instance = x;
                             var filename = config.workingDir + "/" + x.instanceId + ".json";
                             fs.writeFileSync(filename, JSON.stringify(ret))
                             out = filename
                           } else if (item.eventName == "TerminateInstances") {
                             out = time;
                           }
                           var logentry = [time, item.eventName, x.instanceId]
                           if (!retmap.log)
                             retmap.log = []
                           retmap.log.push(logentry);
                           console.log(logentry)
                           var by_id = retmap[x.instanceId]
                           if (!by_id)
                             retmap[x.instanceId] = by_id = {}
                           by_id[item.eventName] = out;
                         })
    } else { // no rs = failed invokation of ec2 command
//      console.log(item)
    }
  }
}

function logItems(log, config, ret) {
  log.Records.forEach(function (x) {logItem(x, config, ret)})
  return ret;
}

function combineOptions(a, b) {
  var ret = {}
  for (var i in a)
    ret[i] = a[i]
  for (var i in b)
    ret[i] = b[i]
  return ret;
}

function s3ListObjects(s3, options, callback) {
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

var S3getObjectGunzip = async.compose(function (data, callback) {
                                  zlib.gunzip(data.Body, callback);
                                },
                                function (params, callback) {
                                  params.s3.getObject(params.params, callback);
                                });

function processS3Log(s3, file, config, ret, callback) {
  S3getObjectGunzip({'s3':s3, 'params':{'Bucket':config.cloudTrailBucket, 'Key':file}},
               function (err, data) {
                 if (err)
                   return callback(err);
                 logItems(JSON.parse(data), config, ret)
                 // empty callback to signify completion
                 // result is passed back via ret
                 callback(null, null);
               })
}

function updateInstanceJSONWithTerminateTime(filename, time) {
  var stuff = JSON.parse(fs.readFileSync(filename));
  stuff.terminateTime = time;
  fs.writeFileSync(filename, JSON.stringify(stuff));
  console.log("updated:"+ filename);
}

/**
 * This function is complicated
 * it:
 * a) sees RunInstances json and schedules it for upload
 * b) folds TerminateInstances timestamps into RunInstances json from this run
 * c) folds TerminateInstances timestamps into RunInstances json from S3(and schedules these for reupload)
 */
function foldTerminations(s3, logEntries, config, callback) {
  function instanceJSONRemote(instanceId) {
    return config.instancePrefix + instanceId + ".json"
  }
  function instanceJSONLocal(instanceId) {
    return config.workingDir + "/" + instanceId + ".json"
  }
  var filesToUpload = {}
  var log = logEntries.log;
  if (log) {
    delete logEntries['log']
    log = log.sort(function (a, b) {return a[0] - b[0]}).join("\n");
    var filename = config.workingDir + "/log.txt"; 
    fs.writeFileSync(filename, log);
    filesToUpload[config.instanceLogPrefix + "info-" + (new Date().getTime()) + ".csv"] = filename;
  }

  var terminatesToResolve = []
  for (var instanceId in logEntries) {
    var entry = logEntries[instanceId]
    var terminateTime = entry['TerminateInstances']
    var run = entry['RunInstances']
    if (terminateTime) {
      if (run) {
        updateInstanceJSONWithTerminateTime(run, terminateTime);
      } else {
        // we failed to find a local json file to put terminate info into
        terminatesToResolve.push([instanceId, terminateTime])
      }
    }
    if (run)
      filesToUpload[instanceJSONRemote(instanceId)] = run
  }
  function resolveTerminate(tuple, callback) {
    var instanceId = tuple[0]
    var terminateTime = tuple[1]
    var remote = instanceJSONRemote(instanceId);
    S3getObjectGunzip({'s3':s3, 'params':{'Bucket':config.outBucket, 'Key':remote}},
               function (err, data) {
                 if (err) {
                   if (err.statusCode == 404) {
                     console.log("404 while looking for " + remote);
                     return callback(null, tuple);
                   }
                   return callback(err);
                 }
                 var filename = instanceJSONLocal(instanceId);
                 fs.writeFileSync(filename, data);
                 updateInstanceJSONWithTerminateTime(filename, terminateTime);         
                 filesToUpload[remote] = filename
                 callback(null, null);
               })
  }
  async.mapLimit(terminatesToResolve, 400, resolveTerminate,
                 function (err, missingTerminates) {
                   if (err)
                     return callback(err);
                   var missingLog = missingTerminates.filter(function (x) {return x != null}).join("\n")
                   var name = "orphanTerminates-" + (new Date().getTime()) + ".csv";
                   var filename = config.workingDir + "/" + name;
                   fs.writeFileSync(filename, missingLog);
                   filesToUpload[config.instanceLogPrefix + name] = filename;
                   callback(null, filesToUpload)
                 })
}

function uploadToS3(s3, config, uploadMap, callback) {
  function uploader(key, callback) {
    var local = uploadMap[key];
    async.waterfall([function(callback) {
                       zlib.gzip(fs.readFileSync(local), callback);
                     },
                     function (gzipbody, callback) {
                       console.log(config.outBucket, key, gzipbody.length);
                       s3.putObject({'Bucket':config.outBucket, 
                                    'Key':key,
                                    'Body': gzipbody,
                                    'ACL':'public-read',
                                    'ContentEncoding':'gzip',
                                    'ContentType':'text/plain'}, callback);
                     }
                    ], callback);
  }
  async.mapLimit(Object.keys(uploadMap), 40, uploader, callback);
}

function chunkArray(array, n ) {
    if ( !array.length ) {
        return [];
    }
    return [ array.slice( 0, n ) ].concat( chunkArray(array.slice(n), n) );
}

function main() {
  var config = JSON.parse(fs.readFileSync("config.json"))
  try {
    fs.mkdirSync(config.workingDir)
  } catch (e) {
    if (e.code != "EEXIST")
      throw e
  }

  var s3 = new AWS.S3({'accessKeyId':config.accessKeyId, 'secretAccessKey':config.secretAccessKey});
  var processedLogs = null;
  //console.log(s3)
  async.waterfall([ function (callback) {
                      s3ListObjects(s3, {'Bucket':config.cloudTrailBucket, 'Prefix':'CloudTrail/'},
                                    function(err, ls) {
                                      var files = ls.filter(function (x) {return x.Size > 0})
                                        .map(function (x) {return x.Key})
                                     callback(null, files)
                                   });
                    },
                    function (files, callback) {
                      var ret = {};
                      async.mapLimit(files, 400, 
                                     function (file, filecallback) {
                                       processS3Log(s3, file, config, ret, filecallback)
                                     },
                                     function (err) {
                                       callback(err, ret);
                                     })
                      processedLogs = files
                    },
                    function (logmap, callback) {
                      foldTerminations(s3, logmap, config, callback);
                    },
                    function (uploadMap, callback) {
                      uploadToS3(s3, config, uploadMap, callback);
                    },
                    function (ignore, callback) {
                      // archive the processed logs
                      console.log('archiving ' + processedLogs.length + " logs");
                      async.mapLimit(processedLogs, 400, 
                                     function (key, callback) {
                                       s3.copyObject({'Bucket':config.cloudTrailBucket,
                                                      'Key': 'archive/' + key,
                                                      'CopySource': config.cloudTrailBucket + "/" + key,
                                                     }, callback);
                                     },
                                     callback)
                    },
                    function (ignore, callback) {
                      //delete processed logs
                      var chunkedBy1000 = chunkArray(processedLogs, 1000);
                      console.log('deleting ' + processedLogs.length + " logs");
                      async.mapLimit(chunkedBy1000, 400,
                                     function (keys, callback) {
                                       var keys = keys.map(function(x){return {'Key':'archive/'+x}})
                                       s3.deleteObjects({'Bucket':config.cloudTrailBucket,
                                                         'Delete':{'Objects':keys}
                                                        }
                                                        ,function (err, data) {
                                                          if (err)
                                                            return callback(err);
                                                          if (data.Errors)
                                                            return callback(data.Errors)
                                                          callback(null, data);
                                                        });
                                     }, callback);
                      
                    }
                    //todo: produce an index in instances/log/
                  ],
                  function (err, result) {
                    if (err)
                      throw err
                    console.log("all done:"+ JSON.stringify(result))
                  }
  )
}

main();
