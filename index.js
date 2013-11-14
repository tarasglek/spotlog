const fs = require('fs');
const knox = require('knox');
const zlib = require('zlib');
const async = require('async');
const outdir = process.argv[3];
const indir = process.argv[2]

function processLog(logfile, outdir, outputFiles) {
	var str = fs.readFileSync(logfile).toString().trim()
	if (!str.length) {
		console.log(logfile + " is empty")
		return
	}

	var loglines = str.split('\n');
	loglines.forEach(function (line) {
		if (line.length && line[0] == '#')
			return;
		var s = line.split("\t");
		var timestamp = new Date(s[0]);
		var nodeid = s[3];
		var charge = s[7].split(' ')[0];
		var payload = (timestamp * 1) + "," + charge + "\n";
		var filename = outdir + "/" + nodeid + ".csv";
		fs.appendFileSync(filename, payload);
		outputFiles[filename] = 1;
	})
}

var outgoing = 0;
function uploadContentToS3(content, s3, remote, callback) {
	outgoing++;
	console.log(outgoing + " outgoing files")
	var req = s3.put(remote, {
		'Content-Length': content.length
			, 'Content-Type': 'application/json'});
	req.on('response', function(res){
			outgoing--;
			if (200 == res.statusCode) {
				callback(null, req.url)
			} else {
				throw new Error("Unknown s3 upload code: " + res.statusCode)
			}
	});
	req.on('error', function(err) {
		console.log("Retrying " + remote + " in 1s");
		setTimeout(function() {
			outgoing--;
			uploadContentToS3(content, s3, remote, callback);
		}, 1000);
	});
	req.end(content);
}


function uploadToS3(files, s3, prefix, limit, callback) {
	function mapperUploadToS3(f, map_callback) {
		var local = fs.readFileSync(f);
		var s3key = prefix + "/" + f.replace(/.*\//, "")
		s3.get(s3key).on('response', function (res, err) {
			var remote = "";
			// No log file on server -> uploads ours, no need to merge
			if (res.statusCode == 404) {
				uploadContentToS3(local, s3, s3key, map_callback);
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
				uploadContentToS3(ret, s3, s3key, map_callback);
			})
			res.on('error', function (error) {
				map_callback(error)
			})
		}).end();
		//console.log(content.toString().split("\n").map(function (x) {return x.split(",")}))
	}
	async.mapLimit(files, limit, mapperUploadToS3, callback)
}

function downloadFile(s3, remoteFile, localFile, callback) {
	function onDownloadError(err) {
		console.log("Error downloading " + remoteFile + ":" + err)
      	setTimeout(function() {
	      	console.log("Retrying download of " + remoteFile);
			downloadFile(s3, remoteFile, localFile, callback);
		}, 1000);
	}
	s3.getFile(remoteFile, function (err, resp) {
	  if (err) {
	  	onDownloadError(err)
	  	return;
	  }
      var writeStream = fs.createWriteStream(localFile);
      
      resp.on('error', onDownloadError);
      resp.on('end', function () {
      	console.log(remoteFile + "=>" + localFile)
      	callback(null, localFile)
      });
      resp.pipe(zlib.createGunzip()).pipe(writeStream);
	})		
}

//function downloadFile(filename, )
function download(s3, files, transformer, arg_callback) {
	async.map(files, 
		function(f, map_callback) {
			downloadFile(s3, f, transformer(f), map_callback)
		}, arg_callback);
}

function retrieveLogs(s3, dest, callback) {
	s3.list({}, function (err, data) {
		if (err)
			return callback(err);

		if (data.IsTruncated)
			return callback(new Error("TODO: implement listing buckets of > 1K"))

		function remote2local_transformer(remote) {
			return dest + "/" + remote.replace(".gz", ".csv")
		}
		var items = data.Contents.map(function (x) {return x.Key});
		download(s3, items, remote2local_transformer, callback)
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
	try {
		fs.mkdirSync(indir)
	} catch (e) {
		if (e.code != "EEXIST")
			throw e
	}
	var s3in = knox.createClient({
   	 key: config.accessKeyId
	  , secret: config.secretAccessKey
	  , bucket: config.inBucket
	});

	var s3out = knox.createClient({
   	 key: config.accessKeyId
	  , secret: config.secretAccessKey
	  , bucket: config.outBucket
	});	
	async.waterfall([
		function (callback) {
			// download stuff from S3
			retrieveLogs(s3in, indir, callback);	
		},
		function (downloaded_files, callback) {
			var outputFiles = {}
			downloaded_files.forEach(function (x) {
				processLog(x, outdir, outputFiles);
			})
			var result = Object.keys(outputFiles);
			//fs.writeFileSync("ls.json", JSON.stringify(result))
			callback(null, result)
		},/*
		function (callback) {
			callback(null, JSON.parse(fs.readFileSync("ls.json")))
		},*/
		function (processed_files, callback) {
			uploadToS3(processed_files, s3out, "spot-prices", 50, callback)
		},
		function (uploads, callback) {
			console.log("final_step:"+uploads)
			callback()
		}
		], 
		function (err, result) {
			console.log("all done:" + result)
		});
}

main();