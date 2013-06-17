var riak = require('..')
, util = require('util')
, expect = require('expect.js')
;

function done(err, res) {
	console.log(util.inspect(err || res, false, 10));

	expect(err).to.not.be.ok();
	expect(res).to.be.ok();

}
// 'RiakIO', function() {
	// 'the imported object', function() {

		// 'exposes a server object', function() {
			var server = riak.server;

			// with a #create function', function() {
				expect(server).to.be.ok();
				expect(riak.server).to.have.property('create');
				expect(riak.server.create).to.be.a('function');

			// and a Server class', function() {
				expect(server).to.be.ok();
				expect(riak.server).to.have.property('Server');
				expect(riak.server.Server).to.be.a('function');

			// 'with a Server object', function() {
				var s = riak()
				, b
				;

				// #listResources will report available routes', function(done) {
					s.listResources(done);

				// #status will report the server status', function(done) {
					s.status(done);

				// #ping is ponged', function(done) {
					s.ping(done);

				// #listBuckets results in an array of bucket names', function(done) {
					s.listBuckets(function(err, res) {
						expect(res).to.be.ok();
						expect(res.success).to.be('OK');
						expect(res.result).to.be.an(Array);
						done(err, res);
					});

				// #bucket gets a bucket object', function(done) {
					s.bucket('examples', function(err, res) {
						expect(res).to.be.ok();
						expect(res.success).to.be('OK');
						expect(res.result).to.be.a(riak.bucket.Bucket);
						b = res.result;
										var items;

						// #keys gets keys from the bucket', function(done) {
							b.keys(null, function(err, res) {
								expect(res).to.be.ok();
								expect(res.success).to.be('OK');
								expect(res.result).to.be.an(Array);
								done(err, res);
							});
						});



