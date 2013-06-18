var filter = require('../lib/key_filters')
, util = require('util')
, log = require('winston')
;

log.info(util.inspect(
	filter.create(filter.intToString)
	));

log.info(util.inspect(
	filter.create(filter.stringToInt)
	));

log.info(util.inspect(
	filter.create(
		filter.or(
			filter.startsWith('beast'),
			filter.create(
				filter.tokenize('-', 2), filter.eq('flit'))
			)
		)
	, false, 10));
