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
		var content = fs.readFileSync(dir + "/" + f);
		uploadContentToS3(content, s3, prefix + "/" + f);
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