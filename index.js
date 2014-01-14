const fs = require('fs');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const async = require('async');
const tarasS3 = require('./taras-s3.js');
const mkdirp = require('mkdirp');
const cluster = require('cluster');

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
function combineObjectWithCachedS3File(config, upload, downloadDict, s3, key, newObj, callback) {
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
      upload(key, localFilename, str, callback);
    }
  ],function (err, data) {
    if (err)
      return callback(err);
    inFlight.callbacks.forEach(function (callback) {callback(null, key)});
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
  if (config.keys) {
    cfg = config.keys;
  }
  var s3 = new AWS.S3(cfg);
  var logReadingS3 = s3;
  
  if (config.logKeys) {
    logReadingS3 = new AWS.S3(config.logKeys);
  }

  var s3Markers = {}
  var uploadDict = {}
  var downloadDict = {}
  function upload(key, localFilename, contents, callback) {
    uploadDict[key] = localFilename;
    fs.writeFile(localFilename, contents, callback);
    console.log(localFilename)
  } 
  
  function uploadAndCache(key, contents, callback) {
    var localFilename = config.workingDir + "/" + config.outBucket + "/" + key;
    var localDir = localFilename.substring(0, localFilename.lastIndexOf('/'));
    async.waterfall([
      function (callback) {
        mkdirp.mkdirp(localDir, callback);
      },
      function (ignore, callback) {
        upload(key, localFilename, contents, callback);
      }
    ], callback);
  }

  // curry some common params
  function updateJSON(key, newObj, callback) {
    return combineObjectWithCachedS3File(config, upload, downloadDict, s3, key, newObj, callback);
  }

  function processCloudTrail(callback) {
    var cfg = {'Bucket': config.cloudTrailBucket,
               'Prefix': config.cloudTrailPrefix
              };
    
    if (s3Markers.CloudTrail)
      cfg['Marker'] = s3Markers.CloudTrail
    tarasS3.S3MapBucket(logReadingS3, cfg, 400,
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
        tarasS3.S3MapBucket(logReadingS3, cfg, 400,
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

  function describeInstances(region, callback) {
    var keys = config.logKeys;
    if (!keys)
      keys = cfg
    if (!keys)
      return callback(new Error("Need keys to call describeInstances"), null)
    var ec2 = new AWS.EC2(tarasS3.combineObjects({"region":region},keys))
    ec2.describeInstances({}, function (err, data) { 
      if (err)
        return callback(err)
      async.parallel([
        function (callback) {
          handleDescribeInstances(data, callback)
        },
        function (callback) {
          uploadAndCache(config.instanceLogPrefix + region + "/DescribeInstances/" + Date.now() + ".json",
                         JSON.stringify(data), callback);
        }
        ], callback);
    });
    function handleDescribeInstances(di, callback) {
      var todo = []

      function looper(instance) {
        if (instance.State.Name == "terminated") {
          todo.push(instance);
        }
      }
      di.Reservations.forEach(function (reservation) {
        reservation.Instances.forEach(looper);
      })
      async.map(todo, function(instance, callback) {
        var key = instanceJSONRemoteFromConfig(instance.InstanceId, config);
        var newStuff = {"State":instance.State,
                        "instance":
                        {
                          "instanceType": instance.InstanceType,
                          "placement":{"availabilityZone":instance.Placement.AvailabilityZone}
                        },
                        "LaunchTime": instance.LaunchTime.getTime()
                       }
        if (instance.StateTransitionReason)
          newStuff.StateTransitionReason = instance.StateTransitionReason
        if (instance.StateReason)
          newStuff.StateReason = instance.StateReason
        updateJSON(key, newStuff, function (err, data) {
          console.log("describeInstances", err, data);
          callback(err, data);
        });
      }, callback)
    }
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
                      var todo = [processCloudTrail, processSpot];
                      config.describeInstances.forEach(function (region) {
                        todo.push(function(callback) {
                          describeInstances(region, callback);
                        })
                      })
                      async.parallel(todo, callback);
                    },
                    function (ignore, callback) {
                      uploadToS3(s3, config, uploadDict, callback);
                      uploadDict = {};
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
                      for (var i = ret.instances.length - 1; i >= 0;i--) {
                        if (ret.instances[i].indexOf(config.instancePrefix) == -1)
                          ret.instances.splice(i, 1);
                      }
                      if (!ret.instances.length) {
                        console.log("Nothing to log");
                        return callback(null, null);
                      }
                      var logName = Date.now() + ".json";
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
                        async.waterfall([
                          function(callback) {
                            uploadAndCache(config.instanceLogPrefix + logName, JSON.stringify(ret), callback);
                          },
                          function (ignore, callback) {
                            uploadToS3(s3, config, uploadDict, callback);
                          },
                          function (callback) {
                            s3.putObject({
                              'Bucket':config.outBucket, 
                              'ACL':'public-read',
                              'ContentType':'text/plain',
                              'Body':logName,
                              'Key':config.instanceLogPrefix + "index.txt"
                            }, callback)
                            console.log("uploading index.txt");
                          }
                        ], callback);
                      })
                    }
                  ],//todo add index/log stuff
                  function (err, result) {
                    if (err)
                      throw err;
                    console.log("all done:"+ JSON.stringify(result))
                    process.exit(0);
                  }
  )
}

// argv check is to enter debug mode if additional args are present
if (cluster.isMaster && process.argv.length == 2) {
  cluster.fork();
  // keep restarting the child
  cluster.on('exit', 
             function(worker, code, signal) {
               var delay = 1000 * 60 * 10;//poll every 15min
	       if (code == 0) {
		 console.log("Looks like worker finished successfully, respawning in ", delay);
	       } else {
		 console.log("Worker failed with code:"+code)
	       }
               setTimeout(function () {
                 cluster.fork();
               }, delay);
	     });
} else {
  main();
}
