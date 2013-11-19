const fs = require('fs');
const knox = require('knox');
const zlib = require('zlib');
const async = require('async');
const cluster = require('cluster');

function debug(msg) {
	if (0)
		console.log.apply(this, arguments)
}

function processLog(logfile, outdir, outputFiles) {
	var str = fs.readFileSync(logfile).toString().trim()
	if (!str.length) {
		throw (logfile + " is empty")
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
	debug(outgoing + " outgoing files")
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
		debug("Retrying " + remote + " in 1s");
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
				return;
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
		//debug(content.toString().split("\n").map(function (x) {return x.split(",")}))
	}
	async.mapLimit(files, limit, mapperUploadToS3, callback)
}

function downloadFile(s3, remoteFile, localFile, callback) {
	function onDownloadError(err) {
		debug("Error downloading " + remoteFile + ":" + err)
      	setTimeout(function() {
	      	debug("Retrying download of " + remoteFile);
			downloadFile(s3, remoteFile, localFile, callback);
		}, 1000);
	}
	s3.getFile(remoteFile, function (err, resp) {
	  if (err) {
	  	onDownloadError(err)
	  	return;
	  }
	  resp.on('error', onDownloadError);

      var writeStream = fs.createWriteStream(localFile);
      
      writeStream.on('finish', function () {
      	//debug(remoteFile + "=>" + localFile)
      	callback(null, {"remote":remoteFile, "local":localFile})
      });
      resp.pipe(zlib.createGunzip()).pipe(writeStream);
	})		
}

//function downloadFile(filename, )
function download(s3, files, transformer, limit, arg_callback) {
	async.mapLimit(files, limit,
		function(f, map_callback) {
			downloadFile(s3, f, transformer(f), map_callback)
		}, arg_callback);
}

function retrieveLogs(s3, dest, limit, callback) {
	s3.list({}, function (err, data) {
		if (err)
			return callback(err);

		if (data.IsTruncated)
			return callback(new Error("TODO: implement listing buckets of > 1K"))

		function remote2local_transformer(remote) {
			return dest + "/" + remote.replace(".gz", ".csv")
		}
		var items = data.Contents.map(function (x) {return x.Key});
		download(s3, items, remote2local_transformer, limit, callback)
	})

}

function s3move(s3, outBucket, outPrefix, files, deleteFiles, callback) {
	var copyAndMove = function (file, callback) {
		var dest = outPrefix + "/" + file
		var copy_handler = function(res){
			if (res.statusCode != 200) {
				return callback(new Error("s3 Copy failed code:"+res.statusCode))
			}
			debug("copied "+ file + " to " + dest)
			callback(null, file)
		}
		var req = s3.copyTo(file, outBucket, dest).on('response', copy_handler).end();
	}
	async.mapLimit(files, 50, copyAndMove, function (err, ls) {
		if (err)
			return callback(err)
		if (deleteFiles && ls.length) {
			console.log("delete from s3:" + ls);
			s3.deleteMultiple(ls, callback)
		} else
			callback(null, ls)
	});
}

function main() {
	var config = JSON.parse(fs.readFileSync("config.json"))

	const outdir = config.outdir || "input";
	const indir = config.indir || "output";
	
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
			retrieveLogs(s3in, indir, 50, callback);	
		},
		function (downloaded_files, callback) {
			var outputFiles = {}
			var logUrls = []
			try {
				downloaded_files.forEach(function (x) {
					processLog(x.local, outdir, outputFiles);
					logUrls.push(x.remote)
				})
			} catch (e) {
				return callback(e, null)
			}
			var result = Object.keys(outputFiles);
			//fs.writeFileSync("ls.json", JSON.stringify(result))
			callback(null, {"processed_files":result, "logUrls":logUrls})
		},/*
		function (callback) {
			callback(null, JSON.parse(fs.readFileSync("ls.json")))
		},*/
		function (data, callback) {
			uploadToS3(data.processed_files, s3out, "spot-prices", 50, function(err, uploaded_ls) {
				if (err)
					callback(err)
				callback(null, data.logUrls)
			})
		},
		function (logUrls, callback) {
			s3move(s3in, config.processedBucket, "archive", logUrls, !config.keepFiles, callback);
		},
		function (uploads, callback) {
			//debug("final_step:"+uploads)
			callback()
		}
		], 
		function (err, result) {
			if (config.loopTimeout) {
				console.log("Will run again in " + config.loopTimeout + "ms")
				setTimeout(process.exit, config.loopTimeout)
			} else {
				console.log("all done(no loopTimeout):" + [err,result])
				process.exit(1);
			}
		});
}

if (cluster.isMaster) {
	cluster.fork();
	// keep restarting the child
	cluster.on('exit', function(worker, code, signal) {
		if (code == 0) {
			debug("Looks like worker finished successfully, respawning");
			cluster.fork();
		} else {
			console.log("Worker failed with code:"+code)
			process.exit(code)
		}
	});
} else {
	main();
}