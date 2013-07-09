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
		value: function save(callback) {
			dbc([typeof callback === 'function'], 'callback must be a function.');
			var ctx = this.__ctx
			, meta = this.__meta
			, k = ctx.calculateKey(this)
			, headers = ctx.prepareHeaders(this, meta)
			, method = (meta && meta.path) ? 'put' : 'post'
			;
			this.__ctx[method]({
				path: ('undefined' !== typeof k) ? ''.concat(k) : '',
				options: {
					headers: headers,
					json: this
				},
				params: { returnbody: true }
			}, handler.bind(this, callback)
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