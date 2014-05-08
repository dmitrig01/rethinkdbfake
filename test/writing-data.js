var r = require(__dirname+'/../lib')({});
var util = require(__dirname+'/util.js');
var assert = require('assert');

var uuid = util.uuid;
var It = util.It;

var uuid = util.uuid;
var dbName, tableName;


It("Init for `writing-data.js`", function* (done) {
    try {
        dbName = uuid();
        tableName = uuid();

        var result = yield r.dbCreate(dbName).run();
        assert.deepEqual(result, {created:1});

        var result = yield r.db(dbName).tableCreate(tableName).run();
        assert.deepEqual(result, {created:1});

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`insert` should work - single insert`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({}).run();
        assert.equal(result.inserted, 1);

        result = yield r.db(dbName).table(tableName).insert(eval('['+new Array(100).join('{}, ')+'{}]')).run();
        assert.equal(result.inserted, 100);


        done();
    }
    catch(e) {
        done(e);
    }
})


It("`insert` should work - batch insert 1`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert([{}, {}]).run();
        assert.equal(result.inserted, 2);

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`insert` should work - batch insert 2`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert(eval('['+new Array(100).join('{}, ')+'{}]')).run();
        assert.equal(result.inserted, 100);

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`insert` should work - with returnVals true`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({}, {returnVals: true}).run();
        assert.equal(result.inserted, 1);
        assert(result.new_val);
        assert.equal(result.old_val, null);

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`insert` should work - with returnVals false`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({}, {returnVals: false}).run();
        assert.equal(result.inserted, 1);
        assert.equal(result.new_val, undefined);
        assert.equal(result.old_val, undefined);

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` should work - with durability soft`", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({}, {durability: "soft"}).run();
        assert.equal(result.inserted, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` should work - with durability hard`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).insert({}, {durability: "hard"}).run();
        assert.equal(result.inserted, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})
/*
It("`insert` should work - testing upsert true`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).insert({}, {upsert: true}).run();
        assert.equal(result.inserted, 1);

        result = yield r.db(dbName).table(tableName).insert({id: result.generated_keys[0], val:1}, {upsert: true}).run();
        assert.equal(result.replaced, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` should work - testing upsert false`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).insert({}, {upsert: false}).run();
        assert.equal(result.inserted, 1);

        result = yield r.db(dbName).table(tableName).insert({id: result.generated_keys[0], val:1}, {upsert: false}).run();
        assert.equal(result.errors, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` should throw if no argument is given", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).insert().run();
    }
    catch(e) {
        if (e.message === "`insert` takes at least 1 argument, 0 provided after:\nr.db(\""+dbName+"\").table(\""+tableName+"\")") {
            done()
        }
        else {
            done(e);
        }
    }
})
*/
It("`insert` work with dates - 1", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({name: "Michel", age: 27, birthdate: new Date()}).run()
        assert.deepEqual(result.inserted, 1);
        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` work with dates - 2", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert([{name: "Michel", age: 27, birthdate: new Date()}, {name: "Sophie", age: 23}]).run()
        assert.deepEqual(result.inserted, 2);
        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` work with dates - 3", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({
            field: 'test',
            field2: { nested: 'test' },
            date: new Date()
        }).run()
        assert.deepEqual(result.inserted, 1);
        done();
    }
    catch(e) {
        done(e);
    }
})
It("`insert` work with dates - 4", function* (done) {
    try {
        result = yield r.db(dbName).table(tableName).insert({
            field: 'test',
            field2: { nested: 'test' },
            date: r.now()
        }).run()
        assert.deepEqual(result.inserted, 1);
        done();
    }
    catch(e) {
        done(e);
    }
})

/*
It("`insert` should throw if non valid option", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).insert({}, {nonValidKey: true}).run();
    }
    catch(e) {
        if (e.message === 'Unrecognized option `nonValidKey` in `insert` after:\nr.db("'+dbName+'").table("'+tableName+'")\nAvailable options are returnVals <bool>, durability <string>, upsert <bool>') {
            done()
        }
        else {
            done(e);
        }
    }
})
It("`replace` should throw if no argument is given", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).replace().run();
    }
    catch(e) {
        if (e.message === "`replace` takes at least 1 argument, 0 provided after:\nr.db(\""+dbName+"\").table(\""+tableName+"\")") {
            done()
        }
        else {
            done(e);
        }
    }
})
It("`replace` should throw if non valid option", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).replace({}, {nonValidKey: true}).run();
    }
    catch(e) {
        if (e.message === 'Unrecognized option `nonValidKey` in `replace` after:\nr.db("'+dbName+'").table("'+tableName+'")\nAvailable options are returnVals <bool>, durability <string>, nonAtomic <bool>') {
            done()
        }
        else {
            done(e);
        }
    }
})
*/
It("`delete` should work`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result.deleted > 0);

        result = yield r.db(dbName).table(tableName).delete().run();
        assert.equal(result.deleted, 0);

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`delete` should work -- soft durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).delete({durability: "soft"}).run();
        assert.equal(result.deleted, 1);


        result = yield r.db(dbName).table(tableName).insert({}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).delete().run();
        assert.equal(result.deleted, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})



It("`delete` should work -- hard durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).delete({durability: "hard"}).run();
        assert.equal(result.deleted, 1);

        
        result = yield r.db(dbName).table(tableName).insert({}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).delete().run();
        assert.equal(result.deleted, 1);

        done();
    }
    catch(e) {
        done(e);
    }
})
/*
It("`delete` should throw if non valid option", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).delete({nonValidKey: true}).run();
    }
    catch(e) {
        if (e.message === 'Unrecognized option `nonValidKey` in `delete` after:\nr.db("'+dbName+'").table("'+tableName+'")\nAvailable options are returnVals <bool>, durability <string>') {
            done()
        }
        else {
            done(e);
        }
    }
})
*/
It("`update` should work - point update`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).update({foo: "bar"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`update` should work - range update`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert([{id: 1}, {id: 2}]).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).update({foo: "bar"}).run();
        assert.equal(result.replaced, 2);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});
        result = yield r.db(dbName).table(tableName).get(2).run();
        assert.deepEqual(result, {id: 2, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`update` should work - soft durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).update({foo: "bar"}, {durability: "soft"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`update` should work - hard durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).update({foo: "bar"}, {durability: "hard"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`update` should work - returnVals true", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).update({foo: "bar"}, {returnVals: true}).run();
        assert.equal(result.replaced, 1);
        assert.deepEqual(result.new_val, {id: 1, foo: "bar"});
        assert.deepEqual(result.old_val, {id: 1});

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`update` should work - returnVals false`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).update({foo: "bar"}, {returnVals: false}).run();
        assert.equal(result.replaced, 1);
        assert.equal(result.new_val, undefined);
        assert.equal(result.old_val, undefined);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
/*
It("`update` should throw if no argument is given", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).update().run();
    }
    catch(e) {
        if (e.message === "`update` takes at least 1 argument, 0 provided after:\nr.db(\""+dbName+"\").table(\""+tableName+"\")") {
            done()
        }
        else {
            done(e);
        }
    }
})
It("`update` should throw if non valid option", function* (done) {
    try{
        result = yield r.db(dbName).table(tableName).update({}, {nonValidKey: true}).run();
    }
    catch(e) {
        if (e.message === 'Unrecognized option `nonValidKey` in `update` after:\nr.db("'+dbName+'").table("'+tableName+'")\nAvailable options are returnVals <bool>, durability <string>, nonAtomic <bool>') {
            done()
        }
        else {
            done(e);
        }
    }
})
*/
It("`replace` should work - point replace`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).replace({id: 1, foo: "bar"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`replace` should work - range replace`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert([{id: 1}, {id: 2}]).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).replace(r.row.merge({foo: "bar"})).run();
        assert.equal(result.replaced, 2);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});
        result = yield r.db(dbName).table(tableName).get(2).run();
        assert.deepEqual(result, {id: 2, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})

It("`replace` should work - soft durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).replace({id: 1, foo: "bar"}, {durability: "soft"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`replace` should work - hard durability`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).replace({id: 1, foo: "bar"}, {durability: "hard"}).run();
        assert.equal(result.replaced, 1);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`replace` should work - returnVals true", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).replace({id: 1, foo: "bar"}, {returnVals: true}).run();
        assert.equal(result.replaced, 1);
        assert.deepEqual(result.new_val, {id: 1, foo: "bar"});
        assert.deepEqual(result.old_val, {id: 1});

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
It("`replace` should work - returnVals false`", function* (done) {
    try {
        var result = yield r.db(dbName).table(tableName).delete().run();
        assert(result);
        result = yield r.db(dbName).table(tableName).insert({id: 1}).run();
        assert(result);

        result = yield r.db(dbName).table(tableName).get(1).replace({id: 1, foo: "bar"}, {returnVals: false}).run();
        assert.equal(result.replaced, 1);
        assert.equal(result.new_val, undefined);
        assert.equal(result.old_val, undefined);

        result = yield r.db(dbName).table(tableName).get(1).run();
        assert.deepEqual(result, {id: 1, foo: "bar"});

        done();
    }
    catch(e) {
        done(e);
    }
})
