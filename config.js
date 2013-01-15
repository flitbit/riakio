var nconf = require('nconf');

nconf.argv().env().file('./config.json');

module.exports = nconf;
