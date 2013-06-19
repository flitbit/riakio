var riak = require('..')
, util   = require('util')
, log    = require('winston')
, request = require('request')
, config = riak.config
, data   = require('./practice-data.json')
;

var server = riak.server.create(config.get('riak:uri'));

function then(fn) {
	return function(err, res) {
		if (err) {
			log.error(util.inspect(err, false, 10));
			process.exit();
		}
		if (fn) fn(res.result);
	}
}

function echo(it) {
	if ('string' === typeof it) {
		log.info(it);
	} else {
		log.info(util.inspect(it, false, 10));
	}
}

function getThumbnail(photo) {

}

function resetSampleData(bucket) {
	var item;
	data.forEach(function(ea) {
		ea.thumbnailURL = 'http://farm'.concat(ea.farm,
			'.staticflickr.com/', ea.server,
			'/', ea.id, '_', ea.secret, '_t.jpg');

		item = bucket.createJsonItem(ea.id, ea);
		item.save(then(getThumbnail));
	});
}

function openBucket() {
	server.bucket('flikr-photos', then(resetSampleData));
}

server.ping(then(openBucket));
