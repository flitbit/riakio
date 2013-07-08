var util  = require('util')
, log     = require('winston')
, nconf   = require('nconf')
, path    = require('path')
, riak    = require('../')
, server  = riak.Server
, filters = riak.KeyFilters
;

// configure riak so we know where to find the server...
var config = nconf.file(path.normalize(path.join(__dirname, './sample-config.json')));
riak(config);

var svr = server.create({ log: log });

function echo(err, res) {
	if (err) log.error(util.inspect(err, false, 10));
	else log.info(util.inspect(res, false, 10));
}

function mapWithPropertyMatching(v, k, a){
	return (v.hasOwnProperty(a.prop) && v[a.prop] == a.value)
		? [v] : [];
}

function firstN(values, n) {
	return values.slice(0, n);
}

function performMapReduce(bucket) {
	bucket.mapred({
		filters: filters.create(filters.endsWith('_6'))
//, phases: [{ map: mapWithPropertyMatching, arg: { prop: 'hello', value: 'world' } } ]
		}
 , echo);
}

function updateItem(item) {
	item.touch_date = (new Date()).toISOString();

	item.save(echo);
}

function getItemsByKey(bucket, keys) {
	var items = bucket.items;

	keys.forEach(function(it) {
		items.byKey(it, function(err, res) {
			echo(err, res);
			if (err) process.exit();

			process.nextTick(function() { updateItem(res.result); })
		});
	});
}

function listBucketKeys(bucket) {
	bucket.keys(null, function(err, res) {
		echo(err, res);
		if (err) process.exit();

		process.nextTick(function() { performMapReduce(bucket); })
		getItemsByKey(bucket, res.result);
	});
}

function setBucketProps(bucket) {
	var props = { allow_mult: false };
	bucket.setProps(props,
		function(err, res) {
			echo(err, res);
			if (err) process.exit();

			process.nextTick(function() { listBucketKeys(bucket); });
		});
}

function getBucketProps(bucket) {
	bucket.getProps(
		function(err, res) {
			echo(err, res);
			if (err) process.exit();

			process.nextTick(function() { setBucketProps(bucket); });
		});
}

svr.bucket('examples', function(err, res) {
	echo(err, res);
	if (err) process.exit();

	process.nextTick(function() { getBucketProps(res.result); });
});
