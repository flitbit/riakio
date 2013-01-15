var util = require('util'),
should   = require('should'),
log      = require('winston'),
extend	 = require('extend'),
config   = require('../config'),
Riak  = require('../index').riak.Riak;

var uri = config.get('riak:uri');

// list the buckets using the raw (base) Riak object...
var riak = new Riak(uri);

