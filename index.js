var Db = require('mongodb').Db,
	Server = require('mongodb').Server,
	ObjectID = require('mongodb').ObjectID,
	ReplSetServers = require('mongodb').ReplSetServers,
	NotFoundError = require('./lib/errors/NotFoundError'),
	ConflictError = require('./lib/errors/ConflictError'),
	_ = require('underscore'),
	async = require('async');

module.exports = function MongoClient(options) {

	options = options || {};
	options.db = options.db || {};

	var isConnecting = false,
		callbacksInWaiting = [],
		log = options.log || function(){},
		statsdClient = options.statsdClient || {increment:function(){},decrement:function(){},counter:function(){},guage:function(){},timing:function(){},getChildClient:function(){return this;}},
		self = this,
		dbHostParts,
		dbName = options.db.name || 'test',
		dbHosts = options.db.hosts || ['127.0.0.1:27017'],
		dbUser = options.db.userName || '',
		dbPass = options.db.password || '',
		collectionName = options.db.collectionName || 'test',
		indices = options.db.indices,
		collection;

	this._db = null;

	if (dbHosts.length === 1) {
		dbHostParts = dbHosts[0].split(':');
		if (dbHostParts.length < 2) {
			dbHostParts.push(27017);
		}
		this._db = options.db.instance || new Db(dbName, new Server(dbHostParts[0], parseInt(dbHostParts[1], 10), { auto_reconnect: true }), { w: 1, native_parser: true });
	}
	else {
		var servers = [];
		for(var i =0; i< dbHosts.length; i++) {
			dbHostParts = dbHosts[i].split(':');
			if (dbHostParts.length < 2) {
				dbHostParts.push(27017);
			}
			servers.push(new Server(dbHostParts[0], parseInt(dbHostParts[1], 10), { auto_reconnect: true } ));
		}
		var replSet = new ReplSetServers(servers);
		this._db = options.db.instance || new Db(dbName, replSet, { w:1, native_parser: true });
	}

	statsdClient = statsdClient.getChildClient('db.' + dbName + '.' + collectionName);


	// protected props start with _
	this._getStatsdClient = function() {
		return statsdClient;
	};

	this._convertToObjectIds = function(ids) {
		if (_.isArray(ids)) {
			return _.map(ids, function(id) { return  (_.isString(id)) ? new ObjectID(id.toLowerCase()) : id; });
		}
		return new ObjectID(ids);
	};

	this._getInstrumentedCallback = function(metric, cb) {
		var startTime = new Date();
		return function(err, result) {
			metric = (err) ? metric + '.error' : metric + '.success';
			statsdClient.increment(metric);
			statsdClient.timing(metric, startTime);
			cb(err, result);
		};
	};

	this.disconnect = function() {
		if (this._db.serverConfig.isConnected()) {
			statsdClient.increment('close-connection');
			this._db.close();
		}
	};

	//TODO: fix callback hell in this func
	this.connect = function(callback) {
		if (this._db.serverConfig.isConnected() && callbacksInWaiting.length < 1 && !isConnecting) {
			return callback(null, collection);
		}
		else if (!this._db.serverConfig.isConnected() && !isConnecting) {
			isConnecting = true;
			var startTime = new Date();
			this._db.open(function(err, db) {
				statsdClient.timing('open-connection', startTime);
				if (err) {
					log('debug', 'Failed to open connection with DB. Collection: ' + collectionName);
					statsdClient.increment('open-connection.failure');
					callbacksInWaiting.forEach(function(cb) {
						cb(err);
					});
					callbacksInWaiting = [];
					callback(err);
					isConnecting = false;
					return;
				}

				try {
					collection = self._db.collection(collectionName);
				}
				catch (ex) {
					log('error', 'Error getting collection "' + collectionName + '"', ex);
					callbacksInWaiting.forEach(function(cb) {
						cb(ex);
					});
					callbacksInWaiting = [];
					callback(ex);
					isConnecting = false;
					return;
				}

				statsdClient.increment('open-connection.success');
				log('debug', 'Connection opened successfully with DB. Collection: ' + collectionName);
				if (dbUser) {
					log('debug', 'Authenticating to DB as user:', dbUser);
					self._db.authenticate(dbUser, dbPass, function(err, result) {
						if (err) {
							callbacksInWaiting.forEach(function(cb) {
								cb(err);
							});
							callbacksInWaiting = [];
							callback(err);
							isConnecting = false;
							return;
						}
						if (indices) {
							log('debug', 'Ensuring indices on collection "' + collectionName + '" :', indices);
							var ensureIndexFuncs = [];
							for (var i = 0; i < indices.length; i++) {
								(function(idx) {
									var func = function(cb) {
										collection.ensureIndex(idx.index, {background: true, safe: true, unique: idx.unique}, cb);
									};
									ensureIndexFuncs.push(func);
								}(indices[i]));
							}
							async.parallel(ensureIndexFuncs, function(err, results) {
								if (err) {
									log('error', 'Error ensuring indices on collection "' + collectionName + '"', err);
									callbacksInWaiting.forEach(function(cb) {
										cb(err);
									});
									callbacksInWaiting = [];
									callback(err);
									isConnecting = false;
									return;
								}
								log('debug', 'Successfully ensured indices on collection "' + collectionName + '"');
								callbacksInWaiting.forEach(function(cb) {
									cb(null, collection);
								});
								callbacksInWaiting = [];
								callback(null, collection);
								isConnecting = false;
							});
						}
						else {
							log('debug', 'No indices on this collection');
							callbacksInWaiting.forEach(function(cb) {
								cb(null, collection);
							});
							callbacksInWaiting = [];
							callback(null, collection);
							isConnecting = false;
						}
					});
				}
				else {
					log('debug', 'No DB authentication being used');
					if (indices) {
						log('debug', 'Ensuring indices on collection "' + collectionName + '" :', indices);
						var ensureIndexFuncs = [];
						for (var i = 0; i < indices.length; i++) {
							(function(idx) {
								var func = function(cb) {
									collection.ensureIndex(idx.index, {background: true, safe: true, unique: idx.unique}, cb);
								};
								ensureIndexFuncs.push(func);
							}(indices[i]));
						}
						async.parallel(ensureIndexFuncs, function(err, results) {
							if (err) {
								log('error', 'Error ensuring indices on collection "' + collectionName + '"', err);
								callbacksInWaiting.forEach(function(cb) {
									cb(err);
								});
								callbacksInWaiting = [];
								callback(err);
								isConnecting = false;
								return;
							}
							log('debug', 'Successfully ensured indices on collection "' + collectionName + '"');
							callbacksInWaiting.forEach(function(cb) {
								cb(null, collection);
							});
							callbacksInWaiting = [];
							callback(null, collection);
							isConnecting = false;
						});
					}
					else {
						callbacksInWaiting.forEach(function(cb) {
							cb(null, collection);
						});
						callbacksInWaiting = [];
						callback(null, collection);
						isConnecting = false;
					}
				}
			});
		}
		else {
			callbacksInWaiting.push(callback);
		}
	};



	//findAll, with optional selector
	this.findAll = function(selector, callback) {
		// if selector is a function, assume it's actually the callback
		if (selector && typeof(selector) === 'function') {
			callback = selector;
			selector = {};
		}
		else if (!callback || typeof(callback) !== 'function' || (selector && typeof(selector) !== 'object')) {
			throw new Error('Missing or invalid parameters');
		}
		// make sure selector isn't null/undefined
		selector = selector || {};

		self.connect(function(error, collection) {
			if (error) {
				callback(error);
				return;
			}
			collection.find(selector).toArray(self._getInstrumentedCallback('find', callback));
		});
	};

	this.find = function(id, callback) {
		if (!id || (typeof(id) !== 'string' && typeof(id) !== 'object') || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		id = (typeof(id) === 'string') ? new ObjectID(id) : id;
		self.findBy({ _id: id }, callback);
	};

	this.findBy = function(selector, callback) {
		if (!selector || typeof(selector) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		self.connect(function(error, collection) {
			if (error) {
				return callback(error);
			}
			var icb = self._getInstrumentedCallback('findOne', callback);
			collection.findOne(selector, function(error, doc) {
				if (error) {
					return icb(error);
				}
				if (!doc) {
					return icb(new NotFoundError('Document Not Found'));
				}
				icb(error, doc);
			});
		});
	};

	this.insert = function(doc, callback) {
		if (!doc || typeof(doc) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		if (doc.hasOwnProperty('_id')) {
			throw new Error('Unable to insert a document that already contains an _id property');
		}

		self.connect(function(error, collection) {
			if (error) {
				return callback(error);
			}
			doc.created = new Date();
			doc.modified = new Date();

			var icb = self._getInstrumentedCallback('insert', callback);
			collection.insert(doc, { w: 1 }, function(error, newDocs) {
				if (error) {
					if (error.message.indexOf('duplicate key error') >= 0) {
						var searchStr = 'index: ' + dbName + '.' + collectionName + '.$';
						var fieldIdx = error.message.indexOf(searchStr);
						var field = error.message.substr(fieldIdx + searchStr.length).split('_')[0];
						error = new ConflictError('Object with property ' + field + '=' + doc[field] + ' already exists');
					}
					return icb(error);
				}
				icb(error, newDocs[0]);
			});
		});
	};
	
	this.update = function(id, doc, callback, leaveModifiedDate) {
		if (!id || !doc || typeof(doc) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		id = (typeof(id) === 'string') ? new ObjectID(id) : id;
		self.updateBy({ _id: id }, [['_id', 1]], doc, callback, leaveModifiedDate);
	};

	this.updateBy = function(selector, sort, doc, callback, leaveModifiedDate) {
		if (!selector || !sort || !doc || typeof(doc) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}

		self.connect(function(error, collection) {
			if (error) {
				return callback(error);
			}

			var icb = self._getInstrumentedCallback('findAndModify', callback);

			delete doc._id;
			delete doc.created;
			if (!leaveModifiedDate) {
				doc.$set = {
					modified: new Date()
				}
			}
			else {
				delete doc.modified;
			}

			collection.findAndModify(selector, sort, doc, { w: 1, new: true, multi: false }, function(error, entry) {
				if (error) {
					if (error.message.indexOf('duplicate key error') >= 0) {
						var searchStr = 'index: ' + dbName + '.' + collectionName + '.$';
						var fieldIdx = error.message.indexOf(searchStr);
						var field = error.message.substr(fieldIdx + searchStr.length).split('_')[0];
						error = new ConflictError('Object with property ' + field + '=' + doc[field] + ' already exists');
					}
					return icb(error);
				}
				if (!entry) {
					return icb(new NotFoundError('Document Not Found'));
				}
				icb(null, entry);
			});
		});
	};

	this.updatePartial = function(id, doc, callback, leaveModifiedDate) {
		if (!id || !doc || typeof(doc) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		id = (typeof(id) === 'string') ? new ObjectID(id) : id;
		self.updatePartialBy({ _id: id }, [['_id', 1]], doc, callback, leaveModifiedDate);
	};

	this.updatePartialBy = function(selector, sort, doc, callback, leaveModifiedDate) {
		if (!selector || !sort || !doc || typeof(doc) !== 'object' || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}

		self.connect(function(error, collection) {
			if (error) {
				return callback(error);
			}

			var icb = self._getInstrumentedCallback('findAndModify', callback);

			delete doc._id;
			delete doc.created;
			if (!leaveModifiedDate) {
				doc.modified = new Date();
			}
			else {
				delete doc.modified;
			}

			collection.findAndModify(selector, sort, { $set: doc }, { w: 1, new: true, multi: false }, function(error, entry) {
				if (error) {
					if (error.message.indexOf('duplicate key error') >= 0) {
						var searchStr = 'index: ' + dbName + '.' + collectionName + '.$';
						var fieldIdx = error.message.indexOf(searchStr);
						var field = error.message.substr(fieldIdx + searchStr.length).split('_')[0];
						error = new ConflictError('Object with property ' + field + '=' + doc[field] + ' already exists');
					}
					return icb(error);
				}
				if (!entry) {
					return icb(new NotFoundError('Document Not Found'));
				}
				icb(null, entry);
			});
		});
	};



	this.remove = function(id, callback) {
		if (!id || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		id = (typeof(id) === 'string') ? new ObjectID(id) : id;
		self.removeBy({ _id: id }, [['_id', 1]], callback);
	};

	this.removeBy = function(selector, sort, callback) {
		if (!selector || !sort || !callback || typeof(callback) !== 'function') {
			throw new Error('Missing or invalid parameters');
		}
		self.connect(function(error, collection) {
			if (error) {
				return callback(error);
			}

			var icb = self._getInstrumentedCallback('findAndRemove', callback);
			collection.findAndRemove(selector, sort, { w: 1 }, function(error, entry) {
				if (error) {
					return icb(error);
				}
				if (!entry) {
					return icb(new NotFoundError('Document Not Found'));
				}
				icb(error, entry);
			});
		});
	};
};


module.exports.NotFoundError = NotFoundError;
module.exports.ConflictError = ConflictError;