const fs = require('fs');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const async = require('async');
const tarasS3 = require('./taras-s3.js');

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

function updateInstanceJSONWithTerminateTime(filename, time) {
  var stuff = JSON.parse(fs.readFileSync(filename));
  stuff.terminateTime = time;
  fs.writeFileSync(filename, JSON.stringify(stuff));
  console.log("updated:"+ filename);
}

function instanceJSONRemoteFromConfig(instanceId, config) {
  return config.instancePrefix + instanceId + ".json"
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
    return instanceJSONRemoteFromConfig(instanceId, config);
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
    tarasS3.S3GetObjectGunzip({'s3':s3, 'params':{'Bucket':config.outBucket, 'Key':remote}},
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
                   if (missingTerminates.length) {
                     var missingLog = missingTerminates.filter(function (x) {return x != null}).join("\n")
                     var name = "orphanTerminates-" + (new Date().getTime()) + ".csv";
                     var filename = config.workingDir + "/" + name;
                     fs.writeFileSync(filename, missingLog);
                     filesToUpload[config.instanceLogPrefix + name] = filename;
                   }
                   callback(null, filesToUpload)
                 })
}

function uploadToS3(s3, config, uploadMap, callback) {
  function uploader(key, callback) {
    var local = uploadMap[key];
    tarasS3.S3GzipPutObject(s3, {'Bucket':config.outBucket, 
                                 'Key':key,
                                 'ACL':'public-read',
                                 'ContentEncoding':'gzip', 
                                 'ContentType':'text/plain'}, 
                            fs.readFileSync(local), callback);
  }
  async.mapLimit(Object.keys(uploadMap), 40, uploader, callback);
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

function uploadSpotData(s3, config, spot_files, callback) {
  function uploader(instanceId, callback) {
    var remote = instanceJSONRemoteFromConfig(instanceId, config)
    tarasS3.S3GetObjectGunzip({'s3':s3, 'params':{'Bucket':config.outBucket, 'Key':remote}},
               function (err, data) {
                 if (err) {
                   // 404 is a normal condition
                   if (err.statusCode == 404) {
                     console.log("Couldn't find " + instanceId + " info to write spot price to...TODO: DescribeInstances");
                     data = {'spotPriceLog':{}}
                   } else {
                     return callback(err);
                   }
                 } else {
                   console.log("Got " + instanceId);
                   data = JSON.parse(data)
                 }
                 var filename = spot_files[instanceId]
                 var spotPriceLog = fs.readFileSync(filename).toString().split("\n");
                 for (var i in spotPriceLog) {
                   var line = spotPriceLog[i].split(",");
                   if (line.length != 2)
                     continue;
                   var timestamp = line[0]
                   var price = line[1]
                   data.spotPriceLog[timestamp] = price
                 }
                 tarasS3.S3GzipPutObject(s3, {'Bucket':config.outBucket, 
                                              'Key': remote,
                                              'ACL':'public-read',
                                              'ContentEncoding':'gzip',
                                              'ContentType':'text/plain'},
                                         JSON.stringify(data), callback);
               })
    
  }
  async.mapLimit(Object.keys(spot_files), 400, uploader, callback);  
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
  var processedLogs = [];
  //console.log(s3)
  async.waterfall([ function (callback) {
                      var ret = {};
                      tarasS3.S3MapBucket(s3, {'Bucket':config.cloudTrailBucket, 'Prefix':'CloudTrail/'}, 400,
                                          function (fileName, fileContents, callback) {
                                            logItems(JSON.parse(fileContents), config, ret);
                                            processedLogs.push(fileName);
                                            callback(null, null);
                                          },
                                          function (err, ignore) {
                                            callback(err, ret);
                                          })
                    },
                    function (logmap, callback) {
                      foldTerminations(s3, logmap, config, callback);
                    },
                    function (uploadMap, callback) {
                      uploadToS3(s3, config, uploadMap, callback);
                    },//todo: instead of moving stuff archive/ subdir, save last listed key for next run
                    function (ignore, callback) {
                      // move processed logs to archive dir
                      console.log('archiving ' + processedLogs.length + " logs");
                      tarasS3.S3Move(s3, {'Bucket':config.cloudTrailBucket}, processedLogs, 400,
                                     function (key) {
                                       return {'Key':'archive/' + key, 'CopySource': config.cloudTrailBucket + "/" + key}
                                     },
                                     function(key) { // debug delete transformer(deletes the stuff we just archived for now)
                                       return {'Key':'archive/' + key}
                                     },
                                     callback);
                    },
                    //produce an index in instances/log/...todo: delete uploaded files from local disk
                    function (ignore, callback) {
                      tarasS3.S3ListObjects(s3, {'Bucket':config.outBucket, 'Prefix':config.instanceLogPrefix},
                                    function(err, ls) {
                                      var files = ls.filter(function (x) {
                                                              return x.Size > 0 && !/index.txt$/.test(x.Key);
                                                            })
                                        .map(function (x) {return x.Key.replace(/.*\//, '')})
                                      var body = files.sort(function (a, b) {return a.localeCompare(b) * -1})
                                        .join("\n");
                                      callback(null, body)
                                    });
                      
                    },
                    zlib.gzip,
                    function (gzipbody, callback) {
                      s3.putObject({'Bucket':config.outBucket, 
                                    'Key': config.instanceLogPrefix + "index.txt",
                                    'Body': gzipbody,
                                    'ACL':'public-read',
                                    'ContentEncoding':'gzip',
                                    'ContentType':'text/plain'}, callback);
                    },// now do spot logs
                    function (ignore, callback) {
                      var ret = {};
                      tarasS3.S3MapBucket(s3, {'Bucket':config.processedBucket, 'Prefix':'archive/'}, 400,
                                          function (fileName, fileContents, callback) {
                                            processSpotLog(fileContents.toString(), config, ret);
                                            processedLogs.push(fileName);
                                            callback(null, null);
                                          },
                                          function (err, ignore) {
                                            callback(err, ret);
                                          })
                    },
                    function (spot_files, callback) {
                      uploadSpotData(s3, config, spot_files, callback);
                    }
                  ],
                  function (err, result) {
                    if (err)
                      throw err
                    console.log("all done:"+ JSON.stringify(result))
                  }
  )
}

main();
