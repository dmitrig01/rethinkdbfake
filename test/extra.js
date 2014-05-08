var r = require(__dirname+'/../lib')({});
var util = require(__dirname+'/util.js');
var assert = require('assert');

var uuid = util.uuid;
var It = util.It;

var uuid = util.uuid;
var dbName, tableName;

It("Anonymous function should throw if they return undefined", function* (done) {
    try {
        yield r.expr(1).do(function() {}).run();
    }
    catch(e) {
        if (e.message === "Annonymous function returned `undefined`. Did you forget a `return`?") {
            done()
        }
        else {
            done(e);
        }
    }
})
/*
It("toString should work", function* (done) {
    try {
        assert.equal(r.expr(1).add(2).toString(), "r.expr(1).add(2)");
        assert.equal(r.expr(1).toString(), "r.expr(1)");
        done();
    }
    catch(e) {
        done(e);
    }
})
*/