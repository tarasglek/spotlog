const cluster = require('cluster');

/** call callback regularly in a new process
 & kill it if it takes longer than delay to run */
function setInterval(callback, delay) {
  process.on('uncaughtException', function (err) {
    console.error((new Date).toUTCString() + ' uncaughtException:', err.message)
    console.error(err.stack)
    process.exit(1)
  })

  if (cluster.isMaster) {
    var wipWorker = null;
    // keep restarting the child
    cluster.on('exit', 
               function(worker, code, signal) {
                 wipWorker = null;
                 
	         if (code == 0) {
		   console.log(Date.now(), "Looks like worker finished successfully");
	         } else {
		   console.log("Worker failed with code:"+code)
	         }
	       });

    function loop() {
      if (wipWorker) {
        wipWorker.kill("SIGKILL");
        console.log("Killing stuck worker");
      }
      wipWorker = cluster.fork();
      setTimeout(loop, delay);
      console.log(Date.now(), "Starting worker");
    }
    loop();
  } else {
    callback();
  }
}

module.exports = {
  setInterval: setInterval
};
