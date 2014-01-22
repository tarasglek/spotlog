var logIterator = require('./log-iterator.js')(debugGet)
var async = require('async')
var fs = require('fs')
var zlib = require('zlib')

function debugGet(url, callback) {
  var fsname = url.replace("http://taras-spot-log-processed.s3.amazonaws.com/", "");
  console.log(fsname)
  fs.readFile(fsname, function (err, maybeCompressed) {
    if (err)
      return callback(err);
    zlib.gunzip(maybeCompressed, function (err, uncompressed) {
      //assume files are not compressed if zlib fails us
      if (err && err.code == 'Z_DATA_ERROR') {
        return callback(null, maybeCompressed);
      }
      return callback(err, uncompressed);
    })
  })
}


function jsonGet(url, callback) {
  function handler(err, content) {
    if (err)
      return callback(err);
    callback(null, {'url':url, content: JSON.parse(content)})
  }
  logIterator.get(url, handler);
}

function url2id(url) {
  return url.replace(/.+\//, "").replace(".json", "");
}

function summarize(id, content) {
  var i = content.instance
  console.log(id, i.instanceType, content.LaunchTime, content.terminateTime);
}

function folder(err, logs) {
  if (err)
    throw err;
  var instances = logs.files.filter(function (x) {return x.indexOf("instances/info") != -1})
  
  async.map(instances, jsonGet,
            function (err, ls) {
              ls.forEach(function(x) {
                var id = url2id(x.url);
                summarize(id, x.content);
              })
            })

  return false
}

logIterator.foldLogs("http://taras-spot-log-processed.s3.amazonaws.com/releng/instances/log/index.txt", folder);
