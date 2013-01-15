'use strict';

function Success(message, result) { 
	if (typeof message !== 'string') {
		throw new TypeError('[String] message must be a string!');
	}
	var state = { message: message };
	if (typeof result !== 'undefined') {
		state.result = result;
	}
	Object.defineProperties(this, {
		__priv: { value: state }
	});
}

Object.defineProperties(Success.prototype, {
	message: {
		get: function() {
			return this.__priv.message;
		},
		enumerable: true
	},
	result: {
		get: function() {
			return this.__priv.result;
		}, 
		enumerable: true
	} 
});

module.exports.Success = Success;
