var util    = require('util')
, dbc       = require('dbc.js')
, extend    = util._extend
, Riak      = require('./riak')
, IndexFilter = require('./index_filter')
, KeyFilters = require('./key_filters')
, SolrFilter = require('./solr_filter')
;

function Search(riak) {
	dbc([riak instanceof Riak], 'riak (argument 0) must be an instance of Riak');
	Object.defineProperty(this, 'riak', { value: riak } );
}

Object.defineProperties(Search.prototype, {

	index: {
		value: function(twoi, phases, callback) {
			dbc([twoi instanceof IndexFilter], "twoi (argument 0) must be an instance of IndexFilter.");
			var mr = {}
			, cb = callback || phases
			;
			mr.inputs = twoi;
			if (Array.isArray(phases)) {
				// arity/3
				mr.phases = phases;
			}
			this.riak.mapred(mr, cb);
		},
		enumerable: true
	},

	solr: {
		value: function(query, callback) {
			this.riak.solr(query, callback);
		},
		enumerable: true
	},

	keys: {
		value: function(filters, callback) {
			var mr = {
				phases: [{
					map: function(v) {
						return [v.key];
					}
				}],
				raw_data: true
			},
			cb = callback
			;
			if ('function' === typeof filters) {
				cb = filters;
			} else if (filters) {
				if (filters instanceof KeyFilters) {
					mr.inputs = { key_filters: filters.encoded };
				} else if (filters instanceof IndexFilter) {
					mr.inputs = filters;
				} else if (filters instanceof SolrFilter) {
					mr.inputs = filters;
				} else {
					mr.inputs = filters;
				}
			}
			this.riak.mapred(mr, cb);
		},
		enumerable: true
	},

	mapred: {
		value: function(spec, phases, callback) {
			var mr = {}
			, cb = callback || phases
			;
			if (Array.isArray(phases)) {
				if (spec instanceof KeyFilters) {
					mr.inputs = { key_filters: spec.encoded };
				} else if (spec instanceof IndexFilter) {
					mr.inputs = spec;
				} else if (spec instanceof SolrFilter) {
					mr.inputs = spec;
				}
				mr.phases = phases;
			} else {
				mr = extend(mr, spec);
			}
			this.riak.mapred(mr, callback);
		},
		enumerable: true
	}

});

module.exports = Search;