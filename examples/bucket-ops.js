var util = require('util')
, log = require('winston')
, config = require('../').config
, server = require('../').server
;

var uri = config.get('riak:uri');

var svr = server.create(uri);

function echo(err, res) {
	if (err) log.error(util.inspect(err, false, 10));
	else log.info(util.inspect(res, false, 10));
}

function mapWithPropertyMatching(v,keyData,arg){
	var data = [];
	if (!v.values[0].metadata['X-Riak-Deleted']) {
		var items = Riak.mapValuesJson(v);
		if (items[0][arg.prop] === arg.value) {
			data.push({
				bucket: v.bucket,
				key: v.key,
			data: items[0]});
		}
	}
	return data;
}

function firstN(values, n) {
	return values.slice(0, n);
}

function performMapReduce(bucket) {
	bucket.mapred({
		phases: [
		{
			map: function(v, k, a) {
				var accum = [];
				if (!v.values[0].metadata['X-Riak-Deleted']) {
					var items = Riak.mapValuesJson(v);
					if (items[0][a.prop] && items[0][a.prop].indexOf(a.value) >= 0) {
						accum.push({
							bucket: v.bucket,
							key: v.key,
							data: items[0]});
					}
				}
				return accum;
			},
			a: { prop: 'what', value: 'example_6' }
		}
		]}
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
