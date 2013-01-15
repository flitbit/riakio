var vows = require('vows'),
should   = require('should'),
util     = require('util'),
config   = require('../config'),
server = require('../index').server;

var loc = config.get('riak:uri');

vows.describe('server').addBatch({
	'When using a riak server': {
		'configured for a nearby instance': {
			topic: function() {
				return server.create(loc);
			},
			'should expose a basic restful API': function(it) {
				it.should.have.property('get').instanceOf(Function);
				it.should.have.property('post').instanceOf(Function);
				it.should.have.property('put').instanceOf(Function);
				it.should.have.property('del').instanceOf(Function);
			},
			'when we ping the server': {
				topic: function(it) {
					should.exist(it);
					this.riakContext = it;
					it.ping(this.callback);
				},
				'should be ponged by the server': function(err, res) {
					should.not.exist(err);
					should.exist(res);
					res.should.have.property('success');
				}
			},
			'when we query stats': {
				topic: function(it) {
					should.exist(it);
					it.stats(this.callback);
				},
				'should receive statistics': function(err, res) {
					should.not.exist(err);
					should.exist(res);
					res.should.have.property('success');
				}
			}
		}
	}
}).export(module);


