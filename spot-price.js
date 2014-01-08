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
  for (v in o.spotPriceLog) {
    console.log(v + " "+name+ " " + o.spotPriceLog[v])
  }
  
}

for (var i = 2;i< process.argv.length;i++)
  compute(process.argv[i])
