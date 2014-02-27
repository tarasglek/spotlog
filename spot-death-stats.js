var fs = require('fs');
var logIterator = require('./log-iterator.js')

function hours(ms) {
  return Math.ceil((ms)/1000/60/60)
}

function processInstance(o, file, callback) {
//  if (!o.spotPriceLog)
  //  return

  var name = file.replace(/.*\//, "").replace(".json", "");

  var ret = {"name":name,
             "total_price":0,
             "terminateTime":o.terminateTime
            }
  if (o.instance) {
    ret.availabilityZone = o.instance.placement.availabilityZone
    ret.type = o.instance.instanceType
  }
  
  if (o.StateReason) {
    ret.shutdown = o.StateReason.Code;
  }
  if (o.spotPriceLog) {
    for (v in o.spotPriceLog) {
      ret.total_price += o.spotPriceLog[v] * 1
    }
    ret.hourly_price = ret.total_price/Object.keys(o.spotPriceLog).length
  }
  console.log(JSON.stringify(ret))
  callback(null, null);
}

function compute(file) {
  try {
    o = JSON.parse(fs.readFileSync(file))
  } catch(e) {
    console.log(file, "failed to parse");
    return
  }
  processInstance(o, file);
}

/*if (process.argv)
  for (var i = 2;i< process.argv.length;i++)
    compute(process.argv[i])
*/

logIterator().map(processInstance, process.argv[2], 1000, "releng/instances/log/", "releng/instances/info/", function () {console.log("all done")});
