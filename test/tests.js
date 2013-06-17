var riak = require('..')
, expect = require('expect.js')
;

describe('RiakIO', function() {
	describe('the imported object', function() {

		describe('exposes a server object', function() {
			var server = riak.server;

			it('with a #create function', function() {
				expect(server).to.be.ok();
				expect(riak.server).to.have.property('create');
				expect(riak.server.create).to.be.a('function');
			});

			it('and a Server class', function() {
				expect(server).to.be.ok();
				expect(riak.server).to.have.property('Server');
				expect(riak.server.Server).to.be.a('function');
			});

			describe('with a Server object', function() {
				var s = riak()
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
						expect(res).to.be.ok();
						expect(res.success).to.be('OK');
						expect(res.result).to.be.an(Array);
						done(err, res);
					});
				});

				describe('when working with buckets', function() {
					var b;

					it('#bucket gets a bucket object', function(done) {
						s.bucket('examples', function(err, res) {
							if (!err && res) b = res;
							done(err, res);
						})
					});

					describe('with a bucket (examples)', function() {
						var items;

						it('#keys gets keys from the bucket', function(done) {
							b.keys(null, function(err, res) {
								expect(res).to.be.ok();
								expect(res.keys).to.be.an(Array);
								done(err, res);
							});
						});

					})
				})


			});

		});
	});



});