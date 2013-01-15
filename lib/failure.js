var oops = require('node-oops')
, defines = oops.create
;

function Failure(error, reason) { 
	var message = error || 'unexpected error';
	Failure.super_.call(this);

	defines(this).enumerable.value('error', message);
	if (reason) {
		defines(this).enumerable.value('reason', reason);
	}
}
oops.inherits(Failure, Error);

defines(module.exports).enumerable
	.value('Failure', Failure)
	.method(function create(e, r) { return new Failure(e, r); })
	;

