var riak  = require('..')
, util    = require('util')
, path    = require('path')
, nconf   = require('nconf')
, winston = require('winston')
, request = require('request')
;

// set up a logger so we can see what is happening...
var log = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' })
    ]
  });

// configure riak so we know where to find the server...
var config = nconf.file(path.normalize(path.join(__dirname, './sample-config.json')));
riak(config);

var server = new riak.Server({ log: log });

function then(fn) {
	return function(err, res) {
		if (err) {
			winston.error(util.inspect(err, false, 10));
			process.exit();
		}
		if (fn) fn(res);
	}
}

function echo(it) {
	if ('string' === typeof it) {
		winston.info(it);
	} else {
		winston.info(util.inspect(it, false, 10));
	}
}

function deleteEach(res) {
	var keys = res.result
	, bucket = this
	;
	keys.forEach(function(k) {
		bucket.items.remove(k, then(echo));
	});
}

function removeAll(res) {
	var bucket = res.result;
	bucket.search.keys(then(deleteEach.bind(bucket)));
}

function openBucket() {
	server.bucket('flikr-photos', then(removeAll));
}

server.ping(then(openBucket));
