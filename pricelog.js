var logIterator = require('./log-iterator.js')(debugGet)
var async = require('async')
var fs = require('fs')
var zlib = require('zlib')

function debugGet(url, callback) {
  var fsname = url.replace("http://taras-spot-log-processed.s3.amazonaws.com/", "");
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

var stats = {'defined':0, 'undefined':0}

function startStopStats(startStop, launchTime, endTime) {
  var ss = startStop

  var stats = {"running":0, "stopped":0, "log":[]}

  // insert create/kill into start/stop

  ss[endTime] = "StopInstances"
  var times = Object.keys(ss).sort()
  var lastState = {"name": "StartInstances", "time":launchTime}
  stats.log.push(lastState)
  times.forEach(function (time) {
    var new_state = ss[time]
    stats.log.push([time, new_state])
    if (new_state == lastState && new_state == "StartInstances")
      return;
    delta = time - lastState.time;
    var statName = new_state == "StopInstances" ? "running" : "stopped"
    //todo account for hours being rounded up during stop
    stats[statName] += delta
    lastState = {"name":new_state, "time":time, "delta":delta}
    stats.log.push(lastState)
  })
  return stats
}

function summarize(id, content, timestamp) {
  var prices = {'m3.xlarge':0.45, 'm1.medium':0.12, 'm1.small':0.01}
  var i = content.instance
  if (!i)
    return
  
  var endTime = content.terminateTime || timestamp
  delete content.instance
  //console.log(JSON.stringify(content))
  stateReason = content.StateReason ? content.StateReason.Code : "alive"
  var launchTime = content.LaunchTime ? content.LaunchTime : content.runTime
  var ss = content.startStop ? startStopStats(content.startStop, content.runTime, endTime) : null
  var uptime = ss ? ss.running : (endTime - launchTime)
  var hours = Math.ceil((uptime)/1000/60/60)
  var cost = null
  if (content.spotPriceLog) {
    cost = 0;
    for (var i in content.spotPriceLog) {
      cost += content.spotPriceLog[i] * 1
    }
  } else {
    cost = hours * prices[i.instanceType]
  }
  cost = Math.ceil(cost * 100)/100;
  
  var az = "unknownZone"
  if (i.placement)
    az = i.placement.availabilityZone

  var type = content.spotPriceLog ? "spot" : "ondemand"
  console.log(id, i.instanceType, hours, cost, stateReason, az, type);
}

function finish() {
  console.log(stats)
}

var dedup = {}
function folder(err, logs) {
  if (err) {
    if (err.code == 'ENOENT') {
      finish();
      console.log("no more log entries")
      return
    }
    console.log(err)
    throw err;
  }

  var instances = logs.files.filter(function (x) {
    if (x in dedup)
      return false
    dedup[x] = 1;
    return x.indexOf("instances/info") != -1
  })
  
  async.map(instances, jsonGet,
            function (err, ls) {
              ls.forEach(function(x) {
                var id = url2id(x.url);
                summarize(id, x.content, logs.timestamp);
              })
            })

  return true
}

logIterator.foldLogs("http://taras-spot-log-processed.s3.amazonaws.com/releng/instances/log/index.txt", folder);
