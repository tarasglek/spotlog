var logIterator = require('./log-iterator.js')()
var async = require('async')

function jsonGet(url, callback) {
  function handler(err, content) {
    if (err)
      return callback(err);
    callback(null, {'url':url, content: JSON.parse(content)})
  }
  logIterator.get(url, handler);
}


function folder(err, logs) {
  var instances = logs.files.filter(function (x) {return x.indexOf("instances/info") != -1})
  
  async.map(instances, jsonGet,
            function (err, ls) {
              ls.forEach(function(x) {
                console.log(JSON.stringify(x))
              })
            })

  return false
}

logIterator.foldLogs("http://taras-spot-log-processed.s3.amazonaws.com/releng/instances/log/index.txt", folder);
