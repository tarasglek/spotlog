(function () {
if (typeof module !== 'undefined' && module.exports) {
  var http = require("http")
  var async = require("async")
  var zlib = require("zlib");
}


function get(url, callback) {
  http.get(url, function(res) {
    var body = new Buffer(res.headers['content-length']*1);
    var offset = 0;

    res.on('data', function (chunk) {
      chunk.copy(body, offset);
      offset += chunk.length;
    });
    res.on('end', function () {
      if (res.statusCode != 200) {
        return callback([res.statusCode, url], body.toString());
      }
      if (res.headers['content-encoding'] == 'gzip')
        return zlib.gunzip(body, callback)

      callback(null, body);
    })

  }).on('error', function(e) {
    callback(e);
  });
}

/**
@get function for downloading urls
@indexUrl eg http://taras-spot-log-processed.s3.amazonaws.com/releng/instances/log/index.txt"
@callback (err, dateStamp, {files, previous, timestamp}) 
*/
function foldLogs(get, indexUrl, callback) {
  var logQueue = async.queue(logFetcher, 1);
  var baseUrl = indexUrl.replace(/[^/]+$/, "");

  function instanceUrlFixer(url) {
    // TODO, fix* stuff needs to be fixed serverside
    return (baseUrl + url).replace("releng/instances/log/","");
  }
  
  function logFetcher(logName, callback) {
    var date = logName.replace(".json", "") * 1;
    //console.log(new Date(date))
        
    get(baseUrl + logName, function (err, content) {
      if (err) { 
        return callback(err);
      }
      var o = JSON.parse(content.toString());
      var fixedUp = {"previous":o.previous, "files":o.instances.map(instanceUrlFixer), "timestamp":date};
      if (callback(null, fixedUp)) {
        logQueue.push(o.previous, callback);
      }
    })
  }

  get(indexUrl, function(err, body) {
    if (err)
      return callback(err)
    logQueue.push(body.toString(), callback)
  })

} 

logIterator = 
  function (httpGet) {
   if (!httpGet)
     httpGet = get;
    
   return {
     foldLogs: function (indexUrl, callback) {
       foldLogs(httpGet, indexUrl, callback)
     },
     get: httpGet
   }
 }

if (typeof module !== 'undefined' && module.exports) {
  module.exports = logIterator
}
// included directly via <script> tag
else {
  this.logIterator = logIterator;
}

})();
