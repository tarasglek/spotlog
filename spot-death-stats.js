const fs = require('fs');


function hours(ms) {
  return Math.ceil((ms)/1000/60/60)
}

function compute(file) {
  try {
    o = JSON.parse(fs.readFileSync(file))
  } catch(e) {
    console.log(file, "failed to parse");
    return
  }
  if (!o.spotPriceLog)
    return

  var name = file.replace(/.*\//, "").replace(".json", "");
  var ret = {"name":name, "total_price":0, "type":o.instance.instanceType,
             "shutdown":o.StateReason.Code, "terminateTime":o.terminateTime,
            "availabilityZone":o.instance.placement.availabilityZone}
  if (o.spotPriceLog) {
    for (v in o.spotPriceLog) {
      ret.total_price += o.spotPriceLog[v] * 1
    }
    ret.hourly_price = ret.total_price/Object.keys(o.spotPriceLog).length
  }
  console.log(JSON.stringify(ret))
  
}

for (var i = 2;i< process.argv.length;i++)
  compute(process.argv[i])
