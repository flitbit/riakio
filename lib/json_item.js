var util  = require('util')
, oops    = require('node-oops')
, dbc     = oops.dbc
, defines = oops.create
, extend  = require('extend')
, Riak    = require('./riak').Riak
, handler = require('./utils').handleItemResult
;

function JsonItem(context, meta, data) {
	dbc([context instanceof Riak], "context must be an instance of Riak.");
	defines(this)
	.value('__ctx', context)
	.value('__meta', ((meta) ? extend({}, meta) : undefined));

	extend(this, data);
}

function save(key, callback) {
	var k, cb;
	if (typeof key === 'function') {
		// called without a key...
		k = this.__meta.key;
		cb = key;
	} else {
		// called with a key?..
		k = key || this.__meta.key;
		cb = callback;
	}
	dbc([typeof cb === 'function'], 'callback must be a function.');

	var path = this.__meta.path;
	// any key overrides the path we may already have...
	if (typeof k !== 'undefined') {
		// coerce it into a string-key...
		path = ''.concat(k);
	}

	var headers = this.__ctx.prepareHeaders(this, this.__meta);
	this.__ctx.put({
		path: path,
		options: {
			headers: headers,
			json: this
		},
		params: { returnbody: true }
	}, handler.bind(this, cb)
	);
}

defines(JsonItem).enumerable.method(save);

module.exports.JsonItem = JsonItem;
module.exports.create = function(context, meta, data) {
	return new JsonItem(context, meta, data);
};
