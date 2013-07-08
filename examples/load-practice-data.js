var riak   = require('..')
, util     = require('util')
, winston  = require('winston')
, request  = require('request')
, nconf    = require('nconf')
, path     = require('path')
, template = require('url-template')
, data     = require('./practice-data.json')
;

// set up a logger so we can see what is happening...
var log = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' })
    ]
  });

// configure riak so we know where to find the server...
var config = nconf.file(path.normalize(path.join(__dirname, './sample-config.json')));
riak(config);

var server = new riak.Server({ log: log });

function then(fn) {
	return function(err, res) {
		if (err) {
			winston.error(util.inspect(err, false, 10));
			process.exit();
		}
		if (fn) fn(res);
	}
}

function echo(it) {
	if ('string' === typeof it) {
		winston.info(it);
	} else {
		winston.info(util.inspect(it, false, 10));
	}
}

function queryBucket(bucket, query) {
	bucket.mapred({
		query: query
	}, then(echo));
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

	queryBucket(bucket, { query: 'title:sunset OR swimsuit' });
}


function ensureFullText(res) {
	var bucket = res.result
	;
	bucket.ensureKvSearch(then(resetSampleData.bind(bucket)));
}

function openBucket() {
	server.bucket('flikr-photos', then(ensureFullText));
}

server.ping(then(openBucket));
