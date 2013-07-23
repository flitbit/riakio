var util  = require('util')
, dbc     = require('dbc.js')
, extend  = util._extend
, utils   = require('./utils')
, Riak    = require('./riak')
, handler = utils.handleItemResult
;

function JsonItem(context, meta, data) {
	dbc([context instanceof Riak.constructor], "context must be an instance of Riak.");
	Object.defineProperties(this, {
		__ctx: { value: context },
		__meta: { value: ((meta) ? extend({}, meta) : {})}
	});
	extend(this, data);
}

Object.defineProperties(JsonItem.prototype, {

	getMetaKey: {
		value: function() {
			return this.__meta.key;
		},
		enumerable: true
	},

	save: {
		value: function save(options, callback) {
			var cb = callback || options
			, ctx = this.__ctx
			, meta = this.__meta
			, k = ctx.calculateKey(this)
			, headers = ctx.prepareHeaders(this, meta)
			, method = (meta && meta.path) ? 'put' : 'post'
			;
			dbc([typeof cb === 'function'], 'callback must be a function.');
			if (options['if-match'] && meta && meta.etag) {
				headers['if-match'] = meta.etag;
			} else if (options['if-modified-since'] && meta && meta['last-modified']) {
				headers['if-modified-since'] = meta['last-modified'];
			}
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