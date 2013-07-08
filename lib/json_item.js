var util  = require('util')
, dbc     = require('dbc.js')
, extend  = require('extend')
, utils   = require('./utils')
, Riak    = require('./riak')
, handler = utils.handleItemResult
;

function JsonItem(context, meta, data) {
	dbc([context instanceof Riak], "context must be an instance of Riak.");
	Object.defineProperties(this, {
		__ctx: { value: context },
		__meta: { value: ((meta) ? extend({}, meta) : {})}
	});
	extend(this, data);
}

Object.defineProperties(JsonItem.prototype, {

	save: {
		value: function save(key, callback) {
			var k
			, cb
			, method = (this.__meta && this.__meta.path) ? 'put' : 'post'
			;
			if (typeof key === 'function') {
				k = this.__meta.key;
				cb = key;
			} else {
				k = key || this.__meta.key;
				cb = callback;
			}
			dbc([typeof cb === 'function'], 'callback must be a function.');
			var headers = this.__ctx.prepareHeaders(this, this.__meta);
			this.__ctx[method]({
				path: ('undefined' !== typeof k) ? ''.concat(k) : '',
				options: {
					headers: headers,
					json: this
				},
				params: { returnbody: true }
			}, handler.bind(this, cb)
			);
		},
		enumerable: true
	}

});

Object.defineProperties(JsonItem, {

	create: {
		value: function(context, meta, data) {
			return new JsonItem(context, meta, data);
		},
		enumerable: true
	}

})

module.exports = JsonItem;