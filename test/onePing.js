var 
util   = require('util'),
config = require('../config'),
riakio = require('../index');

util.log(util.inspect(config.get('riak:uri'), false, 99));

var svr = new riakio.riak.Riak(config.get('riak:uri'));

svr.get({ path: 'ping' }, function(err, res) {
	if (err) {
		util.log(util.inspect(err, false, 99));
	} else {
		util.log(util.inspect(res, false, 99));
	}
});
