var util = require('util'),
expect   = require('expect.js'),
winston  = require('winston'),
extend   = require('extend'),
riak     = require('../index')
config   = require('./example_config')
;

riak(config);

// set up a logger so we can see what is happening...
var log = new (winston.Logger)({
	transports: [
	new (winston.transports.Console)({ level: 'info' })
	]
});

// list the buckets using the raw (base) Riak object...
var svr = riak.riak.create({log: log});

function itemHandler(err, res) {
	expect(err).to.be.an('unknown');
	expect(res).to.be.ok();
	expect(res).to.have.property('meta');
	meta = res.meta;
	expect(meta).to.have.property('headers').and.be.an('object');
	expect(res).to.have.property('body');

	if (meta.statusCode === 300) {
		var item = new riakio.JsonObject(svr, meta);
		res.body.forEach(function(i) {
			extend(item, i.body);
		});
		util.log('resolved: '.concat(util.inspect(item, false, 99)));
		item.__commit(itemHandler);
	}
}

function keysHandler(err, res) {
	should.not.exist(err);
	should.exist(res);
	var bucket = this;
	res.forEach(function(k) {
		bucket.items.byKey(k, itemHandler);
	});
}
function bucketPropHandler(err, res) {
	if (res.body.props.allow_mult) {
		this.setProps({ allow_mult: false }, bucketPropHandler.bind(this));
	}
	this.keys(keysHandler.bind(this));
}

svr.get({ path: 'buckets', params: { buckets: true } }, function(err, res) {
	should.not.exist(err);
	should.exist(res);
	res.should.have.property('meta');
	meta = res.meta;
	meta.should.have.property('method').eql('GET');
	meta.should.have.property('path').eql('buckets');
	meta.should.have.property('statusCode').eql(200);
	meta.should.have.property('headers').instanceOf(Object);
	res.should.have.property('body');
	res.body.should.have.property('buckets').instanceOf(Array);

	log.info('There are '.concat(res.body.buckets.length, ' buckets:'));
	res.body.buckets.forEach(function(b) {
		var bucket = riak.bucket.create(uri, b);
		bucket.getProps(bucketPropHandler.bind(bucket));
	});
});

function map(v,keyData,arg){
	var data = [];
	if (!v.values[0].metadata['X-Riak-Deleted']) {
		var items = Riak.mapValuesJson(v);
		if (items[0][arg.prop] === arg.value) {
			data.push({
				bucket: v.bucket,
				key: v.key,
			data: items[0]});
		}
	}
	return data;
}

var firstN = function(values, n) {
	return values.slice(0, n);
}

var testID = Math.floor((Math.random() * 1000000) + 1);
var bucket = new riak.bucket.create(svr, 'vows-tests');

bucket.mapred({
	phases: [
		{ map: map, arg: { prop: 'name', value: 'anonymous'}},
		{ reduce: firstN, arg: 2 } ] }
	, function(err, res) {
		if (err) {
			log.error(util.inspect(err, false, 99));
			return;
		}
		res.body.forEach(function(item) {
			var data = item.data;
			if (!data.touches) {
				data.touches = [];
			}
			data.touches.push({ test: 'test-'.concat(testID), date: new Date()});
			var bucket = new riak.bucket.Bucket(uri, item.bucket);
			var itm = bucket.createJsonItem(item.key, data);
			itm.save(itemHandler);
		});
	});
