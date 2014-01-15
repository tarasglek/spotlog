var http = require("http")
var async = require("async")
var zlib = require("zlib");

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
@baseUrl eg http://taras-spot-log-processed.s3.amazonaws.com/ 
@period eg 24 * 60 * 60 * 1000;//window of data for 1 day
@LOG_PREFIX eg "releng/instances/log/"
@INFO_PREFIX eg "releng/instances/info/"
*/
function mapLogs(mapper, baseUrl, PERIOD, LOG_PREFIX, INFO_PREFIX) {
  var logQueue = async.queue(logFetcher, 1);
  var instanceQueue = async.queue(instanceFetcher, 300);
  var endTime = 0;
  var startTime = 0;
  var instanceGuard = {};

  function instanceFetcher(url, callback) {
    get(url, function (err, data) {
      if (err) {
        console.log("Fail", url);
        return callback(err);
      }
      try {
        mapper(JSON.parse(data.toString()), url)
      } catch (e) {
        console.log("fail", e);
      }
      callback(null, null);
    });
    //console.log(url)
  }

  function logFetcher(logName, callback) {
    var date = logName.replace(".json", "") * 1;
    //console.log(new Date(date))
    if (!endTime) {
      endTime = date;
      startTime = endTime - PERIOD
    } else if (date < startTime) {
      console.log("Finished narrowing time window");
      return callback(null, null);
    }
        
    
    get(baseUrl + LOG_PREFIX + logName, function (err, content) {
      if (err) { 
        console.log("Didn't find a day of logs to process")
        return callback(err);
      }
      var o = JSON.parse(content.toString());
      logQueue.push(o.previous);
      o.instances.forEach(function (x) {
        if (!instanceGuard[x] && x.indexOf(INFO_PREFIX) == 0) {
          instanceQueue.push(baseUrl + x)
          instanceGuard[x] = true;
        }
      });
      callback(null, null);
    })
  }
  var printer = function (err, body) {
    //Console.log(err, body.toString());
  };

  get(baseUrl + LOG_PREFIX + "index.txt", function(err, body) {
    logQueue.push(body.toString())
  })

} 

module.exports = {
  map: mapLogs
};
