const fs = require('fs');
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

function main() {
	try {
		fs.mkdirSync(outdir)
	} catch (e) {
		if (e.code != "EEXIST")
			throw e
	}
	input = fs.readdirSync(indir);
	input.forEach(function (x) {
		processLog(indir + "/" + x, outdir);
	})
}

main();