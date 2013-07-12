var riak   = require('..')
, util     = require('util')
, winston  = require('winston')
, request  = require('request')
, nconf    = require('nconf')
, path     = require('path')
, template = require('url-template')
, data     = require('./practice-data.json')
, KeyFilters = riak.KeyFilters
, SecondaryIndex = riak.SecondaryIndex
, IndexFilter = riak.IndexFilter
, SolrFilter = riak.SolrFilter
;

// SETUP:

// count the number of times each owner appears in the data
var owner_counts = {}
, server_counts = {}
;
data.forEach(function(ea) {
	if (!owner_counts[ea.owner]) {
		owner_counts[ea.owner] = 1;
	} else {
		owner_counts[ea.owner] += 1;
	}
	if (!server_counts[ea.server]) {
		server_counts[ea.server] = 1;
	} else {
		server_counts[ea.server] += 1;
	}
});

// set up a logger so we can see what is happening...
var log = new (winston.Logger)({
	transports: [
	new (winston.transports.Console)({ level: 'info' })
	]
});

// configure riak so we know where to find the server...
nconf.file(path.normalize(path.join(__dirname, './sample-config.json')));
var config = nconf.get('riakio');

riak(config);

var server = new riak.Server({ log: log });

function then() {
	var fn = Array.prototype.slice.call(arguments);
	return function(err, res) {
		if (err) {
			winston.error(util.inspect(err, true, 10));
			process.exit();
		}
		var i = -1
		, len = fn.length
		;
		while(++i < len) {
			fn[i](res);
		}
	}
}

function echo(it) {
	if ('string' === typeof it) {
		winston.info(it);
	} else {
		winston.info(util.inspect(it, true, 99));
	}
}

function itemsMatchServer(server, res) {
	var items = res.result
	;
	items.forEach(function(item) {
		if (item.server !== server) {
			winston.error("received an item not matching the server");
			process.exit();
		}
	});
}

function itemsMatchOwner(owner, res) {
	var items = res.result
	;
	items.forEach(function(item) {
		if (item.owner !== owner) {
			winston.error("received an item not matching the owner");
			process.exit();
		}
	});
}

function indexSearchByServerHavingMoreThan(bucket, n) {
	var server
	;
	for(server in server_counts) {
		if (server_counts[server] > n) {
			bucket.search.index(
				IndexFilter.create('#/server', 'bin').key(server)
			, then(echo, itemsMatchServer.bind(null, server)));
		}
	}
}

function keyFilterByOwnersHavingMoreThanN(bucket, n) {
	var owner
	, filter
	;
	for(owner in owner_counts) {
		if (owner_counts[owner] > n) {
			bucket.search.mapred( KeyFilters.create(KeyFilters.startsWith(owner))
			, then(echo, itemsMatchOwner.bind(null, owner))
			);
		}
	}
}

function queryBucket(bucket, search) {
	bucket.search.solr({
		q: search
		, index: bucket.name
	 },
		then(echo)
		);
}

function resetSampleData(_) {
	var bucket = this
	, item
	, imageurl = template.parse("http://farm{farm}.staticflickr.com/{server}/{id}_{secret}_b.jpg");
	data.forEach(function(ea) {

// formulate the image url...
		ea.thumbnailURL = imageurl.expand(ea);

// create the item on the bucket...
		item = bucket.createJsonItem(ea);

// store it.
		item.save(then(echo));
	});

// Use key filters to query by owner...
	process.nextTick(function() { keyFilterByOwnersHavingMoreThanN(bucket, 5); });
// Use 2i to query by server...
	process.nextTick(function() { indexSearchByServerHavingMoreThan(bucket, 4); });
// Perform a free-text search...
 	process.nextTick(function() { queryBucket(bucket, 'title:sunset OR swimsuit'); });
}


function ensureFullText(res) {
	var bucket = res.result
	;
	bucket.ensureKvSearch(then(resetSampleData.bind(bucket)));
}

function openBucket() {
	server.bucket({
		bucket: 'flikr-photos',
		calculateKey: function(item) {
			return ''.concat(item.owner, '_', item.id);
		},
		index: [SecondaryIndex.create('/server', 'bin')]
	}, then(ensureFullText));
}

server.ping(then(echo, openBucket));
