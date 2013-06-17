var riak = require('..')
, util   = require('util')
, expect = require('expect.js')
;

function handleRiakRootResponse(err, res) {
	var kind = (err) ? "err" : "res";
	console.log(kind.concat(": ", util.inspect(err || res, false, 10)));

	expect(err).to.not.be.ok();
	expect(res).to.be.ok();

}

// make sure the `../config.json` file properly
// identifies Riak's HTTP API endpoint root.

var server = riak();

server.root(handleRiakRootResponse);