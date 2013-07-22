var riak = require('..')
, expect = require('expect.js')
, nconf    = require('nconf')
, path     = require('path')
;

describe('RiakIO', function() {

// configure riak so we know where to find the server...
	var config = nconf.file(path.normalize(path.join(__dirname, './test-config.json')));
	riak(config);

	describe('the imported object', function() {

		describe('exposes a server object', function() {
			var server = riak.Server;

			it('with a #create function', function() {
				expect(server).to.be.ok();
				expect(server).to.have.property('create');
				expect(server.create).to.be.a('function');
			});

			describe('with a Server object', function() {
				var s = new server({ baseUrl: 'http://riak/', httpSignature: {} })
				;

				it ('#listResources will report available routes', function(done) {
					s.listResources(done);
				});

				it ('#status will report the server status', function(done) {
					s.status(done);
				});

				it ('#ping is ponged', function(done) {
					s.ping(done);
				});

				it ('#listBuckets results in an array of bucket names', function(done) {
					s.listBuckets(function(err, res) {
						if (err) done(err);
						else {
							expect(res).to.be.ok();
							expect(res.success).to.be('OK');
							expect(res.result).to.be.an(Array);
							done();
						}
					});
				});

				describe('when working with buckets', function() {
					var b;

					it('#bucket gets a bucket object', function(done) {
						s.bucket('examples', function(err, res) {
							if (err) done(err);
							else {
								b = res.result;
								done();
							}
						})
					});

					describe('with a bucket (examples)', function() {
						var items;
					})
				})


			});

		});
	});



});