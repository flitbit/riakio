function SolrFilter(query, filter) {
	if ('string' === typeof query) {
		Object.defineProperties(this, {
			query: { value: query, enumerable: true }
		});
	}
	if ('string' === typeof filter) {
		Object.defineProperties(this, {
			filter: { value: filter, enumerable: true }
		});
	}
}

Object.defineProperties(SolrFilter.prototype, {

	filter: {
		value: function(value) {
			return new SolrFilter(this.query, value);
		},
		enumerable: true
	},

	query: {
		value: function(value) {
			return new SolrFilter(value, this.filter);
		},
		enumerable: true
	},
});

Object.defineProperties(SolrFilter, {

	create: {
		value: function(query, filter) {
			return new SolrFilter(query, filter);
		},
		enumerable: true
	}

});

module.exports = SolrFilter;