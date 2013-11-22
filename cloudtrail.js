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

function processS3Log(s3, file, config, ret, callback) {
  async.waterfall([
                    function (callback) {
                      var cfg = {'Bucket':config.cloudTrailBucket, 'Key':file}
                      s3.getObject(cfg, callback);
                    },
                    function (data, callback) {
                      zlib.gunzip(data.Body, callback);
                    },
                    function (data, callback) {
                      logItems(JSON.parse(data), config, ret)
                      callback(null, null);
                    }
                  ], callback)
}

function updateInstanceJSONWithTerminateTime(filename, time) {
  var stuff = JSON.parse(fs.readFileSync(filename));
  stuff.terminateTime = time;
  fs.writeFileSync(filename, JSON.stringify(stuff));
  console.log("updated:"+ filename);
}

function foldTerminations(logEntries, config, callback) {
  var filesToUpload = {}
  var log = logEntries.log;
  if (log) {
    delete logEntries['log']
    log = log.sort(function (a, b) {return a[0] - b[0]}).map(function(x) {return x.toString()}).join("\n");
    var filename = config.workingDir + "/log.txt"; 
    fs.writeFileSync(filename, log);
    filesToUpload["/instances/log/" + (new Date().getTime())] = filename;
  }

  for (var instanceId in logEntries) {
    var entry = logEntries[instanceId]
    var terminateTime = entry['TerminateInstances']
    var run = entry['RunInstances']
    if (terminateTime) {
      if (run) {
        updateInstanceJSONWithTerminateTime(run, terminateTime);
      } else {
        console.log("can't match terminate with run for " + instanceId)
      }
    }
    if (run)
      filesToUpload["/instances/info/" + instanceId + ".json"] = run
  }
  callback(null, filesToUpload)
}

function uploadToS3(s3, config, uploadMap, callback) {
  function uploader(key, callback) {
    var local = uploadMap[key];
    async.waterfall([function(callback) {
                       zlib.gzip(fs.readFileSync(local), callback);
                     },
                     function (gzipbody, callback) {
                       s3.putObject({'Bucket':config.outBucket, 
                                    'Key':key,
                                    'ACL':'public-read',
                                    'Body':gzipbody,
                                    'ContentEncoding':'gzip',
                                    'ContentType':'text/plain'}, callback);
                     }], callback);
  }
  async.mapLimit(Object.keys(uploadMap), 40, uploader, callback);
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
                      s3ListObjects(s3, {'Bucket':config.cloudTrailBucket}, function(err, ls) {
                                      var files = ls.filter(function (x) {return x.Size > 0})
                                        .map(function (x) {return x.Key})
                                    //    .slice(0,100);
                                     callback(null, files)
                                   });
                    },
                    function (files, callback) {
                      var ret = {};
                      async.mapLimit(files, 400, 
                                     function (file, filecallback) {processS3Log(s3, file, config, ret, filecallback)},
                                     function (err) {
                                       callback(err, ret);
                                     })
                      processedLogs = files
                    },
                    function (logmap, callback) {
                      foldTerminations(logmap, config, callback);
                    },
                    function (uploadMap, callback) {
                      uploadToS3(s3, config, uploadMap, callback);
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
