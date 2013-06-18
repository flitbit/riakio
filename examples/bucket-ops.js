var util = require('util')
, log = require('winston')
, config = require('../').config
, server = require('../').server
, filters = require('../').filters
;

var uri = config.get('riak:uri');

var svr = server.create(uri);

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
