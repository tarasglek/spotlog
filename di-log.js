const AWS = require('aws-sdk');
const fs = require('fs');
const async = require('async');
const tarasS3 = require('./taras-s3.js');
var net = require('net');

function describeInstances(region, config, callback) {
  if (!config.DEBUG) {
    var keys = config.logKeys;
    if (!keys)
      keys = cfg
    if (!keys)
      return callback(new Error("Need keys to call describeInstances"), null)
    var ec2 = new AWS.EC2(tarasS3.combineObjects({"region":region},keys))
    ec2.describeInstances({}, function (err, data) { 
      if (err)
        return callback(err)
      
      handleDescribeInstances(data, callback)
    });
  } else {
    handleDescribeInstances(JSON.parse(fs.readFileSync("di-"+region+ ".json")), callback);    
  }
  
  function handleDescribeInstances(di, callback) {
    //fs.writeFileSync("di-"+region+ ".json", JSON.stringify(di))
    var todo = []

    di.Reservations.forEach(function (reservation) {
      reservation.Instances.forEach(function(instance){
        todo.push(instance);
      });
    })
    var summary = {};
    todo.forEach(function(instance) {
      var isSpot = (instance.InstanceLifecycle == "spot")
      //replace last - with a . for easy matching
      var az = instance.Placement.AvailabilityZone.replace(/-([^-]+)$/, ".$1")
      var key = (isSpot ? "ec2.spot." : "ec2.ondemand.") + az + "."
      if (config.DEBUG)
        key = "debug_" + key
      var instanceName = instance.InstanceType.replace('.', '-');
      switch (instance.State.Name) {
      case "running":
        key += instanceName
        break;
      case "terminated":
        // guard against terminated nodes hanging around for multiple di calls
        var deadNodes = config.deadNodes;
        if (!config.deadNodes) {
          deadNodes = {};
          try {
            deadNodes = JSON.parse(fs.readFileSync("deadNodes.json"));
            var cutoff = Date.now() - 24 * 60 * 60 * 1000;
            deadNodes.forEach(function (id) {
              if (deadNodes[id] < cutoff)
                delete cutoff[id]
            })
          } catch(e) {
          }
          config.deadNodes = deadNodes;
        }

        if (instance.InstanceId in deadNodes)
          return;
        deadNodes[instance.InstanceId] = Date.now();

        key += "terminated."
        otherkey = key;

        if (instance.StateReason.Code == "Server.SpotInstanceTermination") {
          otherkey = key + "user";
          key += "spot";
        } else {
          otherkey = key + "spot";
          key += "user";
        }
        key += "." + instanceName;
        otherkey += "." + instanceName;
        // make sure to always report 0s if we report termination rates so there is something to compare
        // convoluted logic so we can fill this in for every AZ. Only need this for
        if (isSpot)
          if (!(otherkey in summary))
            summary[otherkey] = 0;
        break;
      default:
        return;
      }
      if (key in summary)
        summary[key]++
      else
        summary[key] = 1;

      });
    callback(null, summary)
  }
}

function logDescribeInstances(config) {
  var todo = config.describeInstances.map(function (region) {
    return (function(callback) {
      describeInstances(region, config, callback);
    })
  })
  async.parallel(todo, function(err, arr) {
    var ret = []
    arr.forEach(function(summary) {
      Object.keys(summary).forEach(function (key) {
        ret.push(config.carbon.prefix + "." + key + " " + summary[key])
      })
    })
    fs.writeFileSync("deadNodes.json", JSON.stringify(config.deadNodes));
    var str = ret.join("\n");
    var socket = net.createConnection(config.carbon.port, 
                                      config.carbon.host,
                                      function() {
					socket.write(str);
                                        socket.on('end', function() {
                                          process.exit(0);
                                        })
                                        socket.end();
                                        console.log(str);
                                      });

    socket.on('timeout', function() {
      console.log("Timeout connecting to ", config.carbon.host, config.carbon.port);
      socket.destroy();
    })
    socket.setTimeout(30000);
  });
}

module.exports = {
  logDescribeInstances: logDescribeInstances
};

