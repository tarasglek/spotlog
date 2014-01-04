const fs = require('fs');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const async = require('async');
const tarasS3 = require('./taras-s3.js');
const mkdirp = require('mkdirp');

function logItem(config,item, updateJSON, callback) {
  switch(item.eventName) {
    case "TerminateInstances":
    case "RunInstances":
    case "StartInstances":
    case "StopInstances":
    var rs = item.responseElements
    if (rs) {
      function iterator(x, callback) {
        var time = new Date(item.eventTime).getTime();
        var out = null;
        var key = instanceJSONRemoteFromConfig(x.instanceId, config);
        var ret = {}
        if (item.eventName == "RunInstances") {
          // do not report redundant commands
          if(x.instanceState.name == "running")
            return callback(null, null);
          ret = {'runTime':time, 'instance':x}
        } else if (item.eventName == "TerminateInstances") {
          switch(x.previousState.name) {
            // do not report redundant commands
            case "terminated":
            case "shutting-down":
              return callback(null, null);
            default:
              ret = {'terminateTime':time}            
          }
        } else if (item.eventName == "StartInstances" || item.eventName == "StopInstances") {
          ret = {}
          var startStop = {};
          startStop[time] = item.eventName;
          ret['startStop'] = startStop;
        }
        //console.log(key/*,x.previousState.name*/, x.instanceState.name, ret)
        updateJSON(key, ret, callback)
      }
      var iset = rs.instancesSet
      return async.eachLimit(iset.items, 400, iterator, callback)
    } else { // no rs = failed invokation of ec2 command
    }
  }
  callback(null);
}

function logItems(config, log, updateJSON, callback) {
  async.eachLimit(log.Records, 400,
                 function (x, callback) {
                   logItem(config, x, updateJSON, callback);
                 }, callback);
}

function updateInstanceJSONWithTerminateTime(filename, time) {
  var stuff = JSON.parse(fs.readFileSync(filename));
  stuff.terminateTime = time;
  fs.writeFileSync(filename, JSON.stringify(stuff));
  console.log("updated:"+ filename);
}

function instanceJSONRemoteFromConfig(instanceId, config) {
  return config.instancePrefix + instanceId + ".json"
}

function uploadToS3(s3, config, uploadDict, callback) {
  function uploader(key, callback) {
    var local = uploadDict[key];
    tarasS3.S3GzipPutObject(s3, {'Bucket':config.outBucket, 
                                 'Key':key,
                                 'ACL':'public-read',
                                 'ContentEncoding':'gzip', 
                                 'ContentType':'text/plain'}, 
                            fs.readFileSync(local), callback);
  }
  async.eachLimit(Object.keys(uploadDict), 400, uploader, 
                  function(err) {
                    callback(err);
                  });
}

function processSpotLog(fileContents, config, ret) {
  var loglines = fileContents.split('\n');
  loglines.forEach(function (line) {
		     if (!line.length || line[0] == '#')
		       return;
		     var s = line.split("\t");
		     var timestamp = new Date(s[0]);
		     var nodeid = s[3];
		     var charge = s[7].split(' ')[0];
		     var payload = (timestamp * 1) + "," + charge + "\n";
		     var filename = config.workingDir + "/spot-" + nodeid + ".csv";
		     fs.appendFileSync(filename, payload);
		     ret[nodeid] = filename;
	           })
}

function mergeSpotData(updateJSON, config, spot_files, callback) {
  function iterator(instanceId, callback) {
    var key = instanceJSONRemoteFromConfig(instanceId, config)
    var filename = spot_files[instanceId]
    var spotPriceLog = fs.readFileSync(filename).toString().split("\n");
    fs.unlinkSync(filename);
    var ret = {'spotPriceLog':{}}
    for (var i in spotPriceLog) {
      var line = spotPriceLog[i].split(",");
      if (line.length != 2)
        continue;
      var timestamp = line[0]
      var price = line[1]
      ret.spotPriceLog[timestamp] = price
    }
    updateJSON(key, ret, callback);
  }
  async.eachLimit(Object.keys(spot_files), 400, iterator, callback);  
}

/**
 * This looks up a file in local cache, download queue and S3...final result is an updated file in local cache
 * we use downloadDict to guard against race conditions
 * 
*/
function combineObjectWithCachedS3File(config, uploadDict, downloadDict, s3, key, newObj, callback) {
  var localFilename = config.workingDir + "/" + config.outBucket + "/" + key;
  var localDir = localFilename.substring(0, localFilename.lastIndexOf('/'));

  var inFlight = downloadDict[localFilename];
  if (inFlight) {
    //console.log("Download race condition avoided, queued", key, newObj);
    inFlight.obj = tarasS3.combineObjects(newObj, inFlight.obj);
    inFlight.callbacks.push(callback);
    return; // we are done, our callback will get called as part of original inFlight request
  } else {
    downloadDict[localFilename] = inFlight = {'obj':newObj, 'callbacks':[callback]};
  }

  async.waterfall([
    // try to read file from local cache before we go to out to s3
    function (callback) {
      fs.readFile(localFilename, function (err, data) {
        function fallback() {
          var params = {'s3':s3, 'params':{'Bucket': config.outBucket, 'Key':key}};
          return tarasS3.S3GetObjectGunzip(params, function (err, data) {
            if (err) {
              // 404 on s3 means this object is new stuff
              if (err.statusCode == 404)
                return callback(null, {});
              else
                return callback(err);
            }
            callback(null, JSON.parse(data));
          })
        }
        // missing file or invalid json are both reasons for concern
        if (err) {
          return fallback()
        }
        var obj;
        try {
          obj = JSON.parse(data)
        }catch(e) {
          return fallback()
        }
        callback(null, obj);
      });
    },
    function (obj, callback) {
      inFlight.obj = tarasS3.combineObjects(inFlight.obj, obj);
      mkdirp.mkdirp(localDir, callback);
    },
    function(ignore, callback) {
      str = JSON.stringify(inFlight.obj);
      delete downloadDict[localFilename];
      uploadDict[key] = localFilename;
      fs.writeFile(localFilename, str, callback);
    }
  ],function (err, data) {
    if (err)
      return callback(err);
    inFlight.callbacks.forEach(function (callback) {callback(null, null)});
  });
}

function main() {
  var config = JSON.parse(fs.readFileSync("config.json"))
  try {
    fs.mkdirSync(config.workingDir)
  } catch (e) {
    if (e.code != "EEXIST")
      throw e
  }
  
  var cfg = null
  if (config.accessKeyId) {
    cfg = {'accessKeyId': config.accessKeyId,
           'secretAccessKey': config.secretAccessKey};
  }
  var s3 = new AWS.S3(cfg);
  var s3Markers = {}
  var uploadDict = {}
  var downloadDict = {}
  // curry some common params
  function updateJSON(key, newObj, callback) {
    return combineObjectWithCachedS3File(config, uploadDict, downloadDict, s3, key, newObj, callback);
  }

  function processCloudTrail(callback) {
    var cfg = {'Bucket': config.cloudTrailBucket,
               'Prefix': config.cloudTrailPrefix
              };
    
    if (s3Markers.CloudTrail)
      cfg['Marker'] = s3Markers.CloudTrail
    tarasS3.S3MapBucket(s3, cfg, 400,
                        function (fileName, fileContents, callback) {
                          logItems(config,
                                   JSON.parse(fileContents),
                                   updateJSON,
                                   function (err, data) {
                                     callback(err, fileName);
                                   });
                        },
                        function (err, keys) {
                          console.log("Cloudtrail log count:", keys.length);
                          if (err)
                            return callback(err);
                          if (keys.length) {
                            var lastKey = keys[keys.length - 1]
                            console.log(new Date(), "last cloudtail key", lastKey, keys.length)
                            s3Markers['CloudTrail'] = lastKey;
                          } else {
                            console.log("No New CloudTrail");
                          }
                          callback(null);
                        })
    
  }

  function processSpot(callback) {
    async.waterfall([
      function (callback) {
        var ret = {}
        var cfg = {'Bucket':config.spotLogBucket, 'Prefix':config.spotLogPrefix}
        if (s3Markers.spot)
          cfg['Marker'] = s3Markers.spot
        tarasS3.S3MapBucket(s3, cfg, 400,
                            function (fileName, fileContents, callback) {
                              processSpotLog(fileContents.toString(), config, ret);
                              processedLogs.push(fileName);
                              callback(null, fileName);
                            },
                            function (err, keys) {
                              if (keys.length) {
                                var lastKey = keys[keys.length - 1]
                                console.log("last spot key", lastKey, keys.length)
                                s3Markers['spot'] = lastKey;
                              } else {
                                console.log("No New spot");
                              }
                              callback(err, ret);
                            })
      },
      function (spot_files, callback) {
        mergeSpotData(updateJSON, config, spot_files, callback);
      }], callback);
  }

  async.waterfall([ function (callback) {
                      if (config.debugState) {
                        s3Markers = config.debugState;
                        return callback(null, null);
                      }

                      s3.getObject({'Bucket':config.outBucket, 'Key':config.stateKey},
                                  function (err, data) {
                                    // 404 = no saved state yet
                                    if (err && err.statusCode == 404)
                                      return callback(null, null);
                                    if (err)
                                      return callback(err);
                                    s3Markers = JSON.parse(data.Body);
                                    callback(null, null);
                                  });
                    },
                    function (ignore, callback) {
                      async.parallel([processCloudTrail, processSpot],callback);
                    },
                    function (ignore, callback) {
                      uploadToS3(s3, config, uploadDict, callback);
                    },
                    //save state to avoid reprocessing logs next time
                    function (callback) {
                      s3.putObject({'Bucket':config.outBucket, 
                                    'Key': config.stateKey,
                                    'Body': JSON.stringify(s3Markers),
                                    'ACL':'public-read',
                                    'ContentType':'text/plain'}, callback);
                    },
                    function (ignore, callback) {
                      // now log the files that were uploaded
                      // each log entry is a linked list pointing at the previous entry
                      // index.txt points at the head of the list
                      var ret = {"instances": Object.keys(uploadDict)}
                      if (!ret.instances.length) {
                        console.log("Nothing to log");
                        return callback(null, null);
                      }
                      var logName = Date.now() + ".json";
                      var common = {'Bucket':config.outBucket, 
                                    'ACL':'public-read',
                                    'ContentType':'text/plain'
                                    }
                      var indexParam = {'Bucket':config.outBucket, 
                                        'Key':config.instanceLogPrefix + "index.txt"}
                      s3.getObject(indexParam, function (err, data) {
                        if (err && err.statusCode == 404) {
                          // do nothing, no .previous to record
                          console.log("no previous logs")
                        } else if (err) {
                          return callback(err);
                        } else {
                          ret.previous = data.Body.toString();
                        }
                        tarasS3.S3GzipPutObject(s3, tarasS3.combineObjects(common, {'Key':config.instanceLogPrefix + logName,
                                                                            'ContentEncoding':'gzip'}), 
                                                JSON.stringify(ret),
                                                function (err) {
                                                  if (err)
                                                    return callback(err);
                                                  s3.putObject(tarasS3.combineObjects(common, {'Body':logName,
                                                                                       'Key':config.instanceLogPrefix + "index.txt"
                                                                                      }),
                                                               callback);
                                                });
                      
                      })
                    }

                  ],//todo add index/log stuff
                  function (err, result) {
                    if (err)
                      throw err
                    console.log("all done:"+ JSON.stringify(result))
                  }
  )
}

main();
// find -name *json | xargs  jq '.Records | .[] | if .eventName == "RunInstances" then . else {}  end ' > run
