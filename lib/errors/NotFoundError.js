var util = require('util');

function NotFoundError(message, constr) {
	Error.captureStackTrace(this, constr || this);
	this.message = message || 'NotFoundError';
}

util.inherits(NotFoundError, Error);
NotFoundError.prototype.name = 'Not Found Error';

module.exports = NotFoundError;