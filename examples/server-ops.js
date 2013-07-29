var util = require('util')
, config = require('./example_config')
, riak = require('../')(config)
;

var svr = new riak.Server();

function getBucket(name) {
	svr.bucket(name, function(err, res) {
		if (err) {
			console.log('failed to get bucket('.concat(name, '):\n', util.inspect(err, false, 12)));
			process.exit();
		}
		console.log('successfully got bucket('.concat(name, ')'));
	});
}

function listBuckets() {
	svr.listBuckets(function(err, res) {
		if (err) {
			console.log('failed to list buckets:\n'.concat(util.inspect(err, false, 12)));
			process.exit();
		}
		console.log('successfully listed buckets');
		var i;
		for(i = 0; i < res.result.length; i++) {
			var name = res.result[i];
			process.nextTick(getBucket.bind(this, name));
		}

		process.nextTick(getBucket.bind(this, 'bogus'));
	});
}

function checkServerStatus() {
	svr.status(function(err, res) {
		if (err) {
			console.log('failed to get stats from the server:\n'.concat(util.inspect(err, false, 12)));
			process.exit();
		}
		console.log('successfully received stats from the server');
		process.nextTick(listBuckets);
	});
}

svr.ping(function(err, res) {
	if (err) {
		console.log('failed to ping the server:\n'.concat(util.inspect(err, false, 12)));
		process.exit();
	}
	console.log('successfully pinged the server');
	process.nextTick(checkServerStatus);
});
