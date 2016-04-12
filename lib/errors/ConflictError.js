var util = require('util');

function ConflictError(message, constr) {
	Error.captureStackTrace(this, constr || this);
	this.message = message || 'ConflictError';
}

util.inherits(ConflictError, Error);
ConflictError.prototype.name = 'Conflict Error';

module.exports = ConflictError;