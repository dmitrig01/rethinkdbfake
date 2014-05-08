var Promise = require("bluebird");

var helper = require(__dirname+"/helper.js");
var Connection = require(__dirname+"/connection.js");
var Term = require(__dirname+"/term.js");
var Error = require(__dirname+"/error.js");

function r(options) { 
    function r(a) {
        return (new Term()).expr(a);
    }
    r.stack = [];
    r.__proto__ = Term.prototype;
    r.connect = function(options) {
        return Promise.resolve(new Connection(options.db));
    };
    
    r.createPool = function(options) {
        return this;
    };
    r.getPool = function() {
        return {
            getLength: function() { return 0; },
            getAvailableLength: function() { return 0; },
            drain: function() { return Promise.resolve(); },
        };
    };
    return r;
};

module.exports = function(options) {
    return new r(options);
}
