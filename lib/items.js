var util    = require('util')
, dbc       = require('dbc.js')
, Riak      = require('./riak')
, Bucket    = require('./bucket')
, JsonItem  = require('./json_item')
, handler   = require('./utils').handleItemResult
;

function Items(bucket) {
	dbc([bucket instanceof Bucket.constructor], 'bucket (argument 0) must be a Bucket');
	Items.super_.call(this, bucket.__options);
	Object.defineProperty(this, 'bucket', { value: bucket, enumerable: true });
	this.appendPath('buckets/'.concat(encodeURIComponent(bucket.name), '/keys'));
}
util.inherits(Items, Riak);

Object.defineProperties(Items.prototype, {

	jsonBodyTransform: {
		value: function(body, meta) {
			return new JsonItem(this, meta, body);
		},
		enumerable: true
	},

	fetch: {
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

	createJsonItem: {
    value: function(data, key) {
    	return this.bucket.createJsonItem(data, key);
    },
    enumerable: true
  },

	calculateKey: {
    value: function(item) {
    	return this.bucket.calculateKey(item);
    },
    enumerable: true
  },

	prepareHeaders: {
		value: function(body, meta) {
			return this.bucket.prepareHeaders(body, meta);
		},
		enumerable: true
	}

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