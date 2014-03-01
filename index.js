const cluster = require('cluster');
const AWS = require('aws-sdk');
const fs = require('fs');
const async = require('async');
const tarasS3 = require('./taras-s3.js');
const config = JSON.parse(fs.readFileSync("config.json"))
var net = require('net');
var DEBUG = process.argv.length > 2;

function describeInstances(region, callback) {
  if (!DEBUG) {
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
      if (DEBUG)
        key = "debug_" + key
      var instanceName = instance.InstanceType.replace('.', '-');
      switch (instance.State.Name) {
      case "running":
        key += instanceName
        break;
      case "terminated":
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
        // convoluted logic so we can fill this in for every AZ
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


function main() {
  var todo = config.describeInstances.map(function (region) {
    return (function(callback) {
      describeInstances(region, callback);
    })
  })
  async.parallel(todo, function(err, arr) {
    var ret = []
    arr.forEach(function(summary) {
      Object.keys(summary).forEach(function (key) {
        ret.push(config.carbon.prefix + "." + key + " " + summary[key])
      })
    })
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
  //                                      process.exit(0);
                                      });
  });
}

// argv check is to enter debug mode if additional args are present
if (cluster.isMaster && !DEBUG) {
  var wipWorker = null;

  // keep restarting the child
  cluster.on('exit', 
             function(worker, code, signal) {
               wipWorker = null;

	       if (code == 0) {
		 console.log(new Date, "Looks like worker finished successfully, respawning in ");
	       } else {
		 console.log("Worker failed with code:"+code)
	       }
	     });

  function go() {
    if (wipWorker) {
      wipWorker.kill();
      console.log("Killing stuck worker");
    }
    wipWorker = cluster.fork();
    var delay = 1000 * 60
    setTimeout(go, delay);
  }
  go();

} else {
  main();
}
