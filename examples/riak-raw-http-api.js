var request = require('request')
, extend    = require('extend')
, util      = require('util')
, utils     = require('../lib/utils')
, log       = require('winston')
, config    = require('..').config
;

// This is the endpoint where Riak's HTTP API resides,
// ensure the config.json file is accurate before executing.

var uri = config.get('riak:uri');

var options = {
	headers: {
		accept: 'application/json, text/plain'
	}
};

var responseHandler = function (err, res, body) {
  if (err) log.error(util.inspect(err, false, 10));
	else {
		log.info(res.request.sequence.concat(': ',
			res.statusCode,
			' - ',
			util.inspect(body, false, 10)
			));
	}
};

// 1 Bucket Operations

// 1.1 - HTTP List Buckets

var list_buckets = extend({
	method: 'GET',
	uri: uri.concat('buckets?buckets=true'),
	sequence: utils.sequence()
}, options);

log.info('1.1 - HTTP List Buckets');
log.info(util.inspect(list_buckets, false, 10));

request(list_buckets, responseHandler);

// 1.2 - HTTP List Keys

var list_keys = extend({
	method: 'GET',
	uri: uri.concat('buckets/examples/keys?keys=true'),
	sequence: utils.sequence()
}, options);

log.info('1.2 - HTTP List Keys');
log.info(util.inspect(list_keys, false, 10));

request(list_keys, responseHandler);

// 1.3 - HTTP Get Bucket Properties

var get_bucket_properties = extend({
	method: 'GET',
	uri: uri.concat('buckets/examples/props'),
	sequence: utils.sequence()
}, options);

log.info('1.3 - HTTP Get Bucket Properties');
log.info(util.inspect(get_bucket_properties, false, 10));

request(get_bucket_properties, function(err, res, body) {
	if (err) log.error(util.inspect(err, false, 10));
	else {
		log.info(res.request.sequence.concat(': ',
			res.statusCode,
			' - ',
			util.inspect(body, false, 10)
			));

// 1.4 - HTTP Set Bucket Properties

		var json = JSON.parse(body)
		, set_bucket_properties = extend({
			method: 'PUT',
			uri: uri.concat('buckets/examples/props'),
			sequence: utils.sequence(),
			json: json
		}, options);

		json.props.allow_mult = true;

		log.info('1.4 - HTTP Set Bucket Properties');
		log.info(util.inspect(set_bucket_properties, false, 10));

		request(set_bucket_properties, function(err, res, body) {
			if (err) log.error(util.inspect(err, false, 10));
			else {
				log.info(res.request.sequence.concat(': ',
					res.statusCode,
					' - ',
					util.inspect(body || "No content.", false, 10)
					));

// 1.5 - HTTP Reset Bucket Properties

				var reset_bucket_properties = extend({
					method: 'DELETE',
					uri: uri.concat('buckets/examples/props'),
					sequence: utils.sequence()
				}, options);

				log.info('1.5 - HTTP Reset Bucket Properties');
				log.info(util.inspect(reset_bucket_properties, false, 10));

				request(reset_bucket_properties, function(err, res, body) {
					if (err) log.error(util.inspect(err, false, 10));
					else {
						log.info(res.request.sequence.concat(': ',
							res.statusCode,
							' - ',
							util.inspect(body || "No content.", false, 10)
							));
					}
				});
			}
		});
	}
});

// 2 Object/Key Operations

// 2.1 HTTP Fetch Object

var id = utils.sequence();

var fetch_object = extend({
	method: 'GET',
	uri: uri.concat('buckets/examples/keys/'.concat(id)),
	sequence: id
}, options);

log.info('2.1 - HTTP Fetch Object');
log.info(util.inspect(fetch_object, false, 10));

request(fetch_object, function(err, res, body) {
	if (err) log.error(util.inspect(err, false, 10));
	else {
		if (res.statusCode === 404) {
			log.info(res.request.sequence.concat(': ',
				res.statusCode,
				' - Not found; creating it now...'
				));

// 2.2 HTTP Store Object

			var store_object = extend({
				method: 'POST',
				uri: uri.concat('buckets/examples/keys/'.concat(id)),
				sequence: utils.sequence(),
				json: {
					id: id,
					timestamp: (new Date()).toISOString()
				}
			}, options);

			log.info('2.2 - HTTP Store Object');
			log.info(util.inspect(store_object, false, 10));

			request(store_object, function(err, res, body) {
				if (err) log.error(util.inspect(err, false, 10));
				else {
					if (res.statusCode === 204) {
						log.info(res.request.sequence.concat(': ',
							res.statusCode,
							' - ',
							util.inspect(body || "No content.", false, 10)
							));

// 2.3 HTTP Delete Object

						var delete_obj = extend({
							method: 'DELETE',
							uri: uri.concat('buckets/examples/keys/'.concat(id)),
							sequence: utils.sequence(),
						}, options);

						log.info('2.3 - HTTP Delete Object');
						log.info(util.inspect(delete_obj, false, 10));

						request(delete_obj, function(err, res, body) {
							if (err) log.error(util.inspect(err, false, 10));
							else {
								log.info(res.request.sequence.concat(': ',
									res.statusCode,
									' - ',
									util.inspect(body || "No content.", false, 10)
									));
							}
						});
						return;
					}
					log.info(res.request.sequence.concat(': ',
						res.statusCode,
						' - ',
						util.inspect(body, false, 10)
						));
				}
			});
		}
	}
});

// 3 Query Operations

// 3.1 - HTTP Link Walking
// 3.2 - HTTP MapReduce
// 3.3 - Secondary Indexes

// 4 Server Operations

// 4.1 - HTTP Ping
// 4.2 - HTTP Status
// 4.2 - HTTP List Resources

