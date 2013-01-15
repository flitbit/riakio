var vows = require('vows'),
should   = require('should'),
util     = require('util'),
config   = require('../config'),
riak     = require('../index').riak;

var server = config.get('riak:uri');

vows.describe('riakio').addBatch({
	'When using a bare riak object': {
		'configured for a nearby riak server': {
			topic: function () {
				return new riak.create(server);
			},
			'should expose a basic restful API': function(it) {
				it.should.have.property('get').instanceOf(Function);
				it.should.have.property('post').instanceOf(Function);
				it.should.have.property('put').instanceOf(Function);
				it.should.have.property('del').instanceOf(Function);
			},
			'when we ping the server': {
				topic: function (it) {
					should.exist(it);
					this.riakContext = it;
					it.get({ path: 'ping' }, this.callback);
				},
				'should be ponged by the server': function(err, res) {
					should.not.exist(err);
					should.exist(res);
					res.should.have.property('meta');
					res.meta.should.have.property('statusCode').eql(200);
				}
			},
			'when we get the server\'s status': {
				topic: function (it) {
					should.exist(it);
					this.riakContext = it;
					it.get({ path: 'stats' }, this.callback);
				},
				'it should respond': function(err, res) {
					should.not.exist(err);
					should.exist(res);
					res.should.have.property('meta');
					res.meta.should.have.property('statusCode').eql(200);
					res.should.have.property('body').with.property('vnode_gets');
				}
			},
			'when we get the server\'s root': {
				topic: function(it) {
					should.exist(it);
					this.riakContext = it;
					it.get({ path: '/', options: {
						headers: { Accept: 'application/json, mutlipart/mixed' }
					} }, this.callback);
				},
				'it should respond': function(err, res) {
					should.not.exist(err);
					should.exist(res);
					res.should.have.property('meta');
					res.meta.should.have.property('statusCode').eql(200);
				}
			}
		}
	}
}).export(module);

