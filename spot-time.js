const fs = require('fs');

function hours(ms) {
  return Math.ceil((ms)/1000/60/60)
}

var file = process.argv[2]
try {
  o = JSON.parse(fs.readFileSync(file))
} catch(e) {
  console.log(file, "failed to parse");
  process.exit(0);
}
if (!o.spotPriceLog || !o.terminateTime)
  process.exit(0);

var keys = Object.keys(o.spotPriceLog)
var max = keys.reduce(function(a,b){return Math.max(a,b)})
var min = keys.reduce(function(a,b){return Math.min(a,b)})

var runHours = hours(o.terminateTime - min)
var billingHours = keys.length
var freeMinutes = Math.floor(((o.terminateTime - min) - (max - min))/1000/60 - 60)
console.log(file, runHours, keys.length, freeMinutes, new Date(o.terminateTime).toString())
