var util    = require('util')
, oops      = require('node-oops')
, dbc       = oops.dbc
, defines   = oops.create
, riakio    = require('./riak')
, json_item = require('./json_item')
, handler   = require('./utils').handleItemResult
;

function Items(server, bucket, options) {
	dbc([typeof bucket === 'string', bucket.length], 'bucket must be a non-empty string.');
	Items.super_.call(this, server);
	defines(this).enumerable.value('bucket', bucket);
	this.__priv.appendPath('buckets/'.concat(encodeURIComponent(bucket), '/keys'));
	if (options && options.indexes) {
		// TODO:
	}
}
util.inherits(Items, riakio.Riak);

function jsonBodyTransform(body, meta) {
	return json_item.create(this, meta, body);
}


function byKey(key, callback) {
	// coerce the key into a string...
	var stringkey = ''.concat(key);
	this.get({
		path: encodeURIComponent(stringkey),
		options: {
			headers: { Accept: 'application/json, mutlipart/mixed' }
		}
	}, handler.bind(this, callback)
	);
}

function remove(key, callback) {
	// coerce the key into a string...
	var stringkey = ''.concat(key);
	this.del({
		path: encodeURIComponent(stringkey),
		options: {
			headers: { Accept: 'application/json, mutlipart/mixed' }
		}
	}, handler.bind(this, callback)
	);
}

defines(Items).configurable.enumerable
	.method(jsonBodyTransform)
	;
defines(Items).enumerable
	.method(remove)
	.method(byKey)
	;

module.exports.Items = Items;
module.exports.create = function(server, bucket) {
	return new Items(server, bucket);
};
