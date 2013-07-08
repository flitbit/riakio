var util    = require('util')
, dbc       = require('dbc.js')
, Riak      = require('./riak')
, JsonItem  = require('./json_item')
, handler   = require('./utils').handleItemResult
;

function Items(options, bucket) {
	dbc([typeof bucket === 'string', bucket.length], 'bucket must be a non-empty string.');
	Items.super_.call(this, options);
	Object(this, 'bucket', { value: bucket, enumerable: true });
	this.__priv.appendPath('buckets/'.concat(encodeURIComponent(bucket), '/keys'));
	if (options && options.indexes) {
		// TODO:
	}
}
util.inherits(Items, Riak);

Object.defineProperties(Items.prototype, {

	jsonBodyTransform: {
		value: function(body, meta) {
			return new JsonItem(this, meta, body);
		},
		enumerable: true
	},

	byKey: {
		value: function(key, callback) {
// coerce the key into a string...
			var stringkey = ''.concat(key);
			this.get({
				path: encodeURIComponent(stringkey),
				options: {
					headers: { Accept: 'application/json, mutlipart/mixed' }
				}
			}, handler.bind(this, callback)
			);
		},
		enumerable: true
	},

	remove: {
		value: function(key, callback) {
// coerce the key into a string...
			var stringkey = ''.concat(key);
			this.del({
				path: encodeURIComponent(stringkey),
				options: {
					headers: { Accept: 'application/json, mutlipart/mixed' }
				}
			}, handler.bind(this, callback)
			);
		},
		enumerable: true
	},

});

Object.defineProperties(Items, {

	create: {
		value: function(options, bucket) {
			return new Items(options, bucket);
		},
		enumerable: true
	}

})

module.exports = Items;