const AWS = require('aws-sdk');
const fs = require('fs');
const async = require('async');

function reportOneRegion(config, region, summary, callback) {
  config.cloudwatch.region = config.ec2.region = region;

  var cloudwatch = new AWS.CloudWatch(config.cloudwatch);
  var ec2 = new AWS.EC2(config.ec2);

  function getEBSMetric(id, MetricName, callback) {
    const TWO_WEEKS = 1209600000;
    const HOUR = 3600000;
    cloudwatch.getMetricStatistics({
      "Namespace": "AWS/EBS",
      "MetricName": MetricName,
      "Dimensions":[{"Name":"VolumeId", "Value":id}],
      "StartTime": new Date(Date.now() - HOUR),
      "EndTime": new Date(),
      "Period": 60*60, //60 seconds minimum, ask for 1 hour
      "Statistics": ["Sum"]
    }, function (err, res) {
      if (err)
        return callback(err);
      //console.log(res)
      var ret = {"MetricName": MetricName, "Value":null} ;
      if (res.Datapoints && res.Datapoints.length) {
        ret.Value = Math.round(res.Datapoints[0].Sum);
      }
      callback(null, ret);
      //console.log(ret);
    });
  }

  /*
   returns[{ VolumeId: 'vol-e40d7be8',
   VolumeWriteOps: 337,
   VolumeReadOps: 10 }... ]
   */
  function listEBSMetrics(volumes, callback) {
    async.mapLimit(volumes, 500, function (VolumeId, callback) {
      async.map(["VolumeWriteOps", "VolumeReadOps"],
                function (MetricName, callback) {
                  getEBSMetric(VolumeId, MetricName, callback);
                },
                function (err, res) {
                  if (err)
                    return callback(err);
                  var ret = {"VolumeId": VolumeId};
                  ret[res[0].MetricName] = res[0].Value;
                  ret[res[1].MetricName] = res[1].Value;
                  callback(null, ret);
                });
    }, callback)
  }

  function analyzeEBS(err, volumes) {
    if (err)
      return callback(err);

    function handleMetrics(err, res) {
      if (err)
        return callback(err);

      fs.writeFileSync("metrics.json", JSON.stringify(res));
      res.forEach(function (x) {
        var size = volumes[x.VolumeId].Size;
        delete x.VolumeId;
        var entry = summary[size];
        if (!entry) {
          summary[size] = entry = {"Volumes":0};
        }
        entry.Volumes++;
        for (key in x) {
          var value = x[key];
          if (!value)
            continue;
          if (key in entry)
            entry[key] += value;
          else
            entry[key] = value;
        }
      })
      callback(null, summary);
    }
    //handleMetrics(null, JSON.parse(fs.readFileSync("metrics.json")));
    listEBSMetrics(Object.keys(volumes), handleMetrics);
  }

  function listVolumes(callback) {
    ec2.describeVolumes({}, function(err, volumes) {
      if (err)
        return callback(err);
      var ret = {};
      volumes.Volumes.forEach(function (x) {
        var id = x.VolumeId;
        delete x.VolumeId;
        ret[id] = x;
      })
      fs.writeFileSync("ebs_volumes.json", JSON.stringify(ret));
      callback(null, ret);
    });
  }

  //analyzeEBS(null, JSON.parse(fs.readFileSync("ebs_volumes.json")));

  listVolumes(analyzeEBS);
}

if (process.argv.length != 4) {
  console.error("Usage " + process.argv[0] + " " + process.argv[1] + "<config.json> <awsregions separated by comma>");
  process.exit(1);
}

var config = JSON.parse(fs.readFileSync(process.argv[2]));
var regions = process.argv[3].split(",");

function summarize(err, summaries) {
  var summary = summaries[0];
  function iops_sum(Stats) {
    var ret = 0;
    if (Stats.VolumeReadOps)
      ret += Stats.VolumeReadOps;
    if(Stats.VolumeWriteOps)
      ret += Stats.VolumeWriteOps;
    return ret;
  }
  var summary_array = Object.keys(summary).map(function (x) {return {"Size": (x), "Stats":summary[x]} });
  summary_array = summary_array.sort(function (a, b) {
    return iops_sum(b.Stats) - iops_sum(a.Stats);
  });
  summary_array.forEach(function (x) {
    var gb_cost = Math.round(x.Size * x.Stats.Volumes * 5 / 730);//gb per hour cost
    var iops_cost = Math.round(iops_sum(x.Stats) * 5 / 1000000);
    console.log("Storage cost: $"+gb_cost/100, "; IOPS cost: $"+iops_cost/100+";", x);
  })
  //console.log(summary_array);

}

var aggregate_summary = {};
async.map(regions, function (region, callback) {
  reportOneRegion(config, region, aggregate_summary, callback);  
}, summarize);

