const fs = require('fs');

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

var diff = Math.ceil((o.terminateTime - max)/1000/60)
console.log(file, diff, keys.length, new Date(o.terminateTime).toString())
