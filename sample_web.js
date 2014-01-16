function get(url, callback) {
  console.log(url)
  $.ajax(url).done(function (data) {callback(null, data)}).fail(function() {callback("jquery")})
}
function processInstance(o, url, callback) {
  console.log(url);
  callback(null, null);
}

function main() {
  logIterator(get).map(processInstance,
                  "http://taras-spot-log-processed.s3.amazonaws.com/",
                  1,
                  "releng/instances/log/",
                  "releng/instances/info/",
                  function () {console.log("all done")});
}
main();
