var util = require('util'),
should   = require('should'),
log      = require('winston'),
extend	 = require('extend'),
config   = require('../config'),
riakio   = require('../index');

var uri = config.get('riak:uri'); 

// list the buckets using the raw (base) Riak object... 
var riak = new riakio.Riak(uri); 

