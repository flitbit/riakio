var path  = require('path')
, nconf   = require('nconf')
;

// configure riak so we know where to find the server...
nconf.file(path.normalize(path.join(__dirname, './sample-config.json')));
module.exports = nconf.get('riakio');
