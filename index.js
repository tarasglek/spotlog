const fs = require('fs');

const config = JSON.parse(fs.readFileSync("config.json"))
config.DEBUG = process.argv.length > 2;

function loop () 
{
  console.log(Date.now(), "Starting worker");
  const di_log = require('./di-log.js');
  di_log.logDescribeInstances(config);
}

loop();
if (!config.DEBUG) {
  setTimeout(function () {
    console.log("Shutting down to restart to avoid OOM");
    process.exit(0);
  }, 60*1000)
}


process.on('uncaughtException', function (err) {
  console.error((new Date).toUTCString() + ' uncaughtException:', err.message)
  console.error(err.stack)
  process.exit(1)
})
