var Promise = require('bluebird');

function Connection(db) {
	this.db = db;
}

Connection.prototype.close = function() {
	return Promise.resolve();
};
Connection.prototype.use = function(db) {
	this.db = db;
};
Connection.prototype.reconnect = function() {
	return Promise.resolve(this);
};
Connection.prototype.noReplyWait = function() {
	return Promise.resolve(true);
};

module.exports = Connection;