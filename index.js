const fs = require('fs');
const knox = require('knox');
const outdir = process.argv[3];
const indir = process.argv[2]

function processLog(logfile, outdir) {
	var loglines = fs.readFileSync(logfile).toString().trim().split('\n');
	loglines.forEach(function (line) {
		if (line.length && line[0] == '#')
			return;
		var s = line.split("\t")
		var timestamp = new Date(s[0])
		var nodeid = s[3]
		var charge = s[7].split(' ')[0]
		var payload = (timestamp * 1) + "," + charge + "\n"
		fs.appendFileSync(outdir + "/" + nodeid + ".csv", payload );
	})
}

var outgoing = 0;
function uploadContentToS3(content, s3, remote) {
	outgoing++;
	console.log(outgoing + " outgoing files")
	var req = s3.put(remote, {
		'Content-Length': content.length
			, 'Content-Type': 'application/json'});
	req.on('response', function(res){
			if (200 == res.statusCode) {
			console.log('saved to %s', req.url);
			}
			outgoing--;
	});
	req.on('error', function(err) {
		console.log("Retrying " + remote + " in 1s");
		setTimeout(function() {
			outgoing--;
			uploadContentToS3(content, s3, remote);
		}, 1000);
	});
	req.end(content);
}

function uploadToS3(dir, s3, prefix) {
	var files = fs.readdirSync(dir);
	files.forEach(function (f){
		var local = fs.readFileSync(dir + "/" + f);
		var s3key = prefix + "/" + f
		s3.get(s3key).on('response', function (res, err) {
			var remote = "";
			// No log file on server -> uploads ours, no need to merge
			if (res.statusCode == 404) {
				res.end();
				uploadContentToS3(local, s3, s3key);
			}
			res.on('data', function (data) {
				remote += data.toString();
			})
			res.on('end', function () {
				var total = {};
				// merge remote and new local entry to avoid duplicates
				// todo, sort keys before output
				(remote+local).trim().split("\n").forEach(function (line) {
					var s = line.split(",");
					total[s[0]] = s[1]
				});
				var ret = ""
				for (var key in total) {
					ret += key + "," + total[key] + "\n"
				}
				uploadContentToS3(ret, s3, s3key);
			})
			res.on('error', function (error) {
				throw error
			})
		}).end();
		//console.log(content.toString().split("\n").map(function (x) {return x.split(",")}))
	})
}

function main() {
	var config = JSON.parse(fs.readFileSync("config.json"))
	try {
		fs.mkdirSync(outdir)
	} catch (e) {
		if (e.code != "EEXIST")
			throw e
	}
	var client = knox.createClient({
   	 key: config.accessKeyId
	  , secret: config.secretAccessKey
	  , bucket: config.outBucket
	});
	input = fs.readdirSync(indir);
	input.forEach(function (x) {
		processLog(indir + "/" + x, outdir);
	})
	uploadToS3(outdir, client, "spot-prices")
}

main();