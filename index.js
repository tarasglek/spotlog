const interval_cluster = require('./taras-interval-cluster.js');
const fs = require('fs');

const config = JSON.parse(fs.readFileSync("config.json"))
config.DEBUG = process.argv.length > 2;

function loop () 
{
  const di_log = require('./di-log.js');
  di_log.logDescribeInstances(config);
}

if (config.DEBUG) {
  loop();
} else {
  interval_cluster.setInterval(loop, 60 * 1000);
  setTimeout(function () {
    console.log("Shutting down to restart to avoid OOM");
    process.exit(0);
  }, 60*60*1000)
}
