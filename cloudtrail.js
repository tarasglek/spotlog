const fs = require('fs');

function logItem(item) {
  switch(item.eventName) {
    case "TerminateInstances":
    case "RunInstances":
//    console.log(item.responseElements.instancesSet)
    var rs = item.responseElements
    if (rs) {
      var iset = rs.instancesSet
      iset.items.forEach(function (x) {
                           // x
                           console.log(item.eventTime,  item.eventName, x.instanceId)
                         })
    } else {
//      console.log(item)
    }
  }
}

function yank(log) {
  log.Records.forEach(logItem)
}

var log = null
var f = process.argv[2]
try {
  log = JSON.parse(fs.readFileSync(f));
} catch(e) {
  console.log(f, e)
}
if (log)
  yank(log)
