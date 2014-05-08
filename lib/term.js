var Cursor = require(__dirname+"/cursor.js");
var Error = require(__dirname+"/error.js");
var datastore = require(__dirname+"/datastore.js");
var _ = require('lodash');
var helper = require(__dirname+"/helper.js");

var Promise = require("bluebird");
var _ = require('lodash');

function Term(stack) {
    var self = this;
    var term = function() {
        return Term.prototype['(...)'].apply(term, arguments);
    }
    term.__proto__ = self.__proto__;
    term.stack = stack || [];

    return term;
}

Term.prototype.run = function(conn, options) {
    try {
        var res = (new Runner(conn, this.stack)).run();
        return Promise.resolve(res);
    }
    catch (e) {
        return Promise.reject(e);
    }
}

function func(name, noPrefix, args, cb) {
    var min = 0, max = 0;
    args = _.map(args, function(arg) {
        var control = arg.substr(arg.length - 1, 1), rest = arg.substr(0, arg.length - 1);
        if (control == '?') {
            max++;
            return { name: rest, control: '?' };
        }
        else if (control == '+') {
            max = 'Infinity';
            min++;
            return { name: rest, control: '+' };
        }
        else if (control == '*') {
            max = 'Infinity';
            return { name: rest, control: '*' };
        }
        else {
            min++;
            max++;
            return { name: arg, control: null };
        }
    });

    Term.prototype[name] = function() {
        if (noPrefix && this.stack.length > 0) throw new Error.ReqlDriverError("`"+name+"` is not defined", this);

        if (min == max && arguments.length != min) {
            throw new Error.ReqlDriverError("`"+name+"` takes "+min+" argument"+((min>1)?'s':'')+", "+arguments.length+" provided", this);
        }
        else if (arguments.length < min) {
            throw new Error.ReqlDriverError("`"+name+"` takes at least "+min+" argument"+((min>1)?'s':'')+", "+arguments.length+" provided", this);
        }
        else if (arguments.length > max) {
            throw new Error.ReqlDriverError("`"+name+"` takes at most "+max+" argument"+((max>1)?'s':'')+", "+arguments.length+" provided", this);
        }

        var options = {}, currentArguments = Array.prototype.slice.call(arguments);
        _.each(args, function(arg) {
            if (arg.control == '?') {
                if (currentArguments.length) {
                    options[arg.name] = currentArguments.shift();
                }
            }
            else if (arg.control == '+' || arg.control == '*') {
                options[arg.name] = currentArguments;
            }
            else {
                options[arg.name] = currentArguments.shift();
            }
        });

        var stack = _.clone(this.stack);
        var main = function() {
            stack.push({
                name: name,
                options: options
            });
            return new Term(stack);
        };

        if (cb) {
            return cb.call(this, options, main);
        }
        else {
            return main();
        }

    };
}

func('dbCreate', true, [ 'db' ]);
func('dbDrop', true, [ 'db' ]);
func('dbList', true, []);

func('tableCreate', false, [ 'table', 'options?' ]);
func('tableDrop', false, [ 'table' ]);
func('tableList', false, [ ]);

func('indexList', false, [ ]);
func('indexCreate', false, [ 'name', 'fn?', 'options?' ]);
func('indexDrop', false, [ 'name' ]);
func('indexStatus', false, [ 'indexes*' ]);
func('indexWait', false, [ 'indexes*' ]);

// Writing data
func('insert', false, [ 'documents', 'options?' ]);


func('update', false, [ 'newValue', 'options?' ]);
func('replace', false, [ 'newValue', 'options?' ]);
func('delete', false, [ 'options?' ]);
func('sync', false, []);

// Selecting data
func('db', true, [ 'db' ]);
func('table', false, [ 'table', 'options?' ]);
func('get', false, [ 'key' ]);
func('getAll', false, [ 'keys+' ]);
func('between', false, [ 'start', 'end', 'options?' ]);
func('filter', false, [ 'filter', 'options?' ]);

// Joins
func('innerJoin', false, [ 'sequence', 'predicate' ]);
func('outerJoin', false, [ 'sequence', 'predicate' ]);
func('eqJoin', false, [ 'leftKey', 'sequence', 'predicate?' ]);
func('zip', false, []);

// Transformation
func('map', false, [ 'transformation' ]);
Term.prototype.withFields = function() {
    var ret = this.hasFields.apply(this, arguments);
    return ret.pluck.apply(ret, arguments);
};
Term.prototype.concatMap = function(mappingFunction) {
    if (!mappingFunction) throw new Error.ReqlDriverError("`concatMap` takes 1 argument, 0 provided");
    return this.map(mappingFunction).reduce(function(a, b) {
        return b.union(a);
    })._inCursor();
};
func('_inCursor', false, []);
Term.prototype.zip = function() {
    return this.map(function(doc) {
        return doc('left').merge('right');
    });
};
func('orderBy', false, [ 'fields+' ]);

function SortOrder(direction, field) {
    this.direction = direction;
    this.field = field;
}
Term.prototype.desc = function(field) {
    return new SortOrder('desc', field);
};
Term.prototype.asc = function(field) {
    return new SortOrder('asc', field);
};

func('skip', false, [ 'value' ]);
func('limit', false, [ 'value' ]);
func('slice', false, [ 'start', 'end', 'options?' ]);
func('nth', false, [ 'value' ]);
func('indexesOf', false, [ 'predicate' ]);
func('isEmpty', false, []);
func('union', false, [ 'other' ]);
func('sample', false, [ 'size' ]);

// Aggregations
func('reduce', false, [ 'func' ]);
func('count', false, [ 'filter?' ]);
func('distinct', false, []);
func('group', false, [ 'groups+' ]);
func('ungroup', false, []);
func('contains', false, [ 'args+' ]);
func('sum', false, []);
func('avg', false, []);
func('min', false, []);
func('max', false, []);

// Document manipulation
func('pluck', false, [ 'fields+' ]);
func('without', false, [ 'fields+' ]);
func('merge', false, [ 'arg' ]);
func('literal', true, [ 'obj' ]);
func('append', false, [ 'value' ]);
func('prepend', false, [ 'value' ]);
func('difference', false, [ 'other' ]);
func('setInsert', false, [ 'other' ]);
func('setUnion', false, [ 'other' ]);
func('setIntersection', false, [ 'other' ]);
func('setDifference', false, [ 'other' ]);
func('(...)', false, [ 'field' ]);
func('hasFields', false, [ 'fields+' ]);
func('insertAt', false, [ 'index', 'value' ]);
func('spliceAt', false, [ 'index', 'array?' ]);
func('deleteAt', false, [ 'start', 'end?' ]);
func('changeAt', false, [ 'index', 'value?' ]);
func('keys', false, []);
func('object', false, [ 'args+' ]);

// String
func('match', false, [ 'regex' ]);
func('upcase', false, []);
func('downcase', false, []);

// Math and Logic
func('add', false, [ 'args+' ]);
func('sub', false, [ 'args+' ]);
func('mul', false, [ 'args+' ]);
func('div', false, [ 'args+' ]);
func('mod', false, [ 'args+' ]);
func('and', false, [ 'args+' ]);
func('or', false, [ 'args+' ]);
func('eq', false, [ 'args+' ]);
func('ne', false, [ 'args+' ]);
func('gt', false, [ 'args+' ]);
func('ge', false, [ 'args+' ]);
func('lt', false, [ 'args+' ]);
func('le', false, [ 'args+' ]);
func('not', false, []);

// Dates and times
func('now', false, []);
func('time', true, []);
func('epochTime', true, [ 'epochTime' ]);
func('ISO8601', true, [ 'isoTime', 'options' ]);
func('inTimezone', false, [ 'timezone' ]);
func('timezone', false, []);
func('during', false, [ 'left', 'right', 'options' ]);
func('date', false, []);
func('timeOfDay', false, []);
func('year', false, []);
func('month', false, []);
func('day', false, []);
func('dayOfYear', false, []);
func('dayOfMonth', false, []);
func('hours', false, []);
func('minutes', false, []);
func('seconds', false, []);
func('toISO8601', false, []);
func('toEpochTime', false, []);

// Control structures
func('do', false, [ 'func' ]);
func('branch', false, [ 'predicate', 'trueBranch', 'falseBranch' ]);
func('forEach', false, [ 'func' ]);
func('error', false, [ 'str' ]);
func('default', false, [ 'value' ]);
func('expr', true, [ 'expression' ], function(options, main) {
    if (options.expression instanceof Term) return options.expression;
    return main();
});
func('js', false, [ 'js' ]);
func('coerceTo', false, [ 'type' ]);
func('typeOf', false, []);
func('info', false, []);
func('json', true, [ 'json' ]);
func('info', false, []);

Term.prototype.row = new Term();

function unimplemented() {}

function Runner(conn, stack, current) {
    this.stack = _.clone(stack);
    this.conn = conn;
    this.context = {};
    if (typeof current != 'undefined') this.current = current;
}

function clone(obj){
    if(obj == null || (!helper.isPlainObject(obj) && !Array.isArray(obj)))
        return obj;

    var temp = obj.constructor(); // changed

    for(var key in obj)
        temp[key] = clone(obj[key]);
    return temp;
}

Runner.prototype.run = function() {
    // JSON.parse(JSON.stringify( = non-fancy deep clone
    var res = clone(this._run());
    if (this.context.inCursor) return new Cursor(res);
    return res;
};

Runner.prototype._run = function() {
    var item = this.stack.shift();
    if (!item) return this.current;

    this.current = this[item.name](item.options);
    if (this.stack.length) return this._run();
    else return this.current;
};

Runner.prototype.toRaw = function(value, current) {
    if (typeof value == 'undefined') return null;

    // Don't really know of a better way of testing if it's a term wihtout using instanceof.
    if (value instanceof Term) value = (new Runner(this.conn, value.stack, current || this.current))._run();

    var self = this;
    if (Array.isArray(value)) {
        return _.map(value, function(val) {
            return self.toRaw(val, current || this.current);
        });
    }
    else if (helper.isPlainObject(value)) {
        var ret = {};
        _.forOwn(value, function(v, k) {
            ret[k] = self.toRaw(v, current || this.current);
        });
        return ret;
    }
    return value;
};
Runner.prototype.exec = function() {
    var args = Array.prototype.slice.call(arguments), func = args.shift();

    // Support plucking syntax.
    if (typeof func == 'string') {
        func = (new Term).row(func);
    }

    if (func instanceof Term) {
        return this.toRaw(func, this.toRaw(args[0]));
    }
    else if (typeof func == 'function') {
        var ret = func.apply(null, args);
        if (typeof ret == 'undefined') throw new Error.ReqlRuntimeError("Annonymous function returned `undefined`. Did you forget a `return`?");
        return this.toRaw(ret);
    }
    else {
        throw "Unable to run function";
    }
};
Runner.prototype.evaluateIndex = function(document, index, val) {
    var table = datastore[this.context.db || this.conn.db][this.context.table];
    var indexVal;
    if (index == table.primaryKey) {
        indexVal = document[index];
    }
    else {
        var index = table.indexes[index];
        if (!index) throw new Error.ReqlRuntimeError('Invalid index');
        if (index.fn) indexVal = this.exec(index.fn, (new Term()).expr(document));
        else indexVal = document[index.name];
    }
    if (val) {
        if (index.options && index.options.multi) {
            return _.indexOf(indexVal, val) != -1;
        }
        else return indexVal == val;
    }
    return indexVal;
}

Runner.prototype.dbCreate = function(options) {
    var db = this.toRaw(options.db);
    if (!db.match(/^[A-Za-z0-9_]+$/)) throw new Error.ReqlRuntimeError('Database name `' + db + '` invalid (Use A-Za-z0-9_ only)');
    if (datastore[db]) {
        throw new Error.ReqlRuntimeError("Database already exists");
    }
    datastore[db] = {};
    return { created: 1 };
};

Runner.prototype.dbDrop = function(options) {
    var db = this.toRaw(options.db);
    if (!datastore[db]) {
        throw new Error.ReqlRuntimeError("Database doesn't exists");
    }
    delete datastore[db];
    return { dropped: 1 };
};

Runner.prototype.dbList = function() {
    this.context.inCursor = true;
    return _.keys(datastore);
};

Runner.prototype.tableCreate = function(options) {
    var db = datastore[this.context.db || this.conn.db];
    var tableName = this.toRaw(options.table);
    if (!tableName.match(/^[A-Za-z0-9_]+$/)) throw new Error.ReqlRuntimeError('Table name `' + tableName + '` invalid (Use A-Za-z0-9_ only)');

    var options = this.toRaw(options.options) || {};

    if (db[tableName]) {
        throw new Error.ReqlRuntimeError('Table exists');
    }
    db[tableName] = {
        data: {},
        indexes: {},
        primaryKey: options.primaryKey || 'id'
    }

    return { created: 1 };
};

Runner.prototype.tableDrop = function(options) {
    var db = datastore[this.context.db || this.conn.db];

    var tableName = this.toRaw(options.table);
    if (!db[tableName]) {
        throw new Error.ReqlRuntimeError('Table does not exist');
    }
    delete db[tableName];

    return { dropped: 1 };
};

Runner.prototype.tableList = function(options) {
    var db = datastore[this.context.db || this.conn.db];
    this.context.inCursor = true;
    return _.keys(db);
};

Runner.prototype.indexList = function(options) {
    this.context.inCursor = true;
    return _.keys(this.context.indexes);
};

Runner.prototype.indexCreate = function(options) {
    if ((options.options == null) && (helper.isPlainObject(options.fn))) {
        options.options = options.fn;
        options.fn = undefined;
    }
    datastore[this.context.db || this.conn.db][this.context.table].indexes[options.name] = options;
    this.context.inCursor = false;
    return { created: 1 };
};

Runner.prototype.indexDrop = function(options) {
    delete datastore[this.context.db || this.conn.db][this.context.table].indexes[options.name];
    this.context.inCursor = false;
    return { dropped: 1 };
};

Runner.prototype.indexStatus =
Runner.prototype.indexWait = function(options) {
    this.context.inCursor = true;
    var indexes = datastore[this.context.db || this.conn.db][this.context.table].indexes;
    if (!options.indexes.length) {
        return _.map(indexes, function(index) {
            return { index: index.name, ready: true };
        });
    }
    else {
        return _.map(options.indexes, function(index) {
            if (!indexes[index]) throw new Error.ReqlRuntimeError('Index does not exist');
            return { index: index, ready: true };
        });
    }
}


// Writing data
Runner.prototype.insert = function(options) {
    var table = datastore[this.context.db || this.conn.db][this.context.table];

    var inserted = 0;
    if (!Array.isArray(options.documents)) options.documents = [ options.documents ];
    var keys = [], new_val;
    _.each(this.toRaw(options.documents), function(document) {
        if (!document[table.primaryKey]) {
            var key = uuid();
            document[table.primaryKey] = key;
            keys.push(key);
        }
        table.data[document[table.primaryKey]] = document;
        new_val = document;
        inserted++;
    });

    this.context.inCursor = false;
    var ret = {
        inserted: inserted,
        generated_keys: keys
    };
    if (options.options && options.options.returnVals) {
        ret.new_val = new_val;
        ret.old_val = null;
    }
    return ret;
}

Runner.prototype.update = function(options) {
    var table = datastore[this.context.db || this.conn.db][this.context.table];

    var func = (new Term()).expr(options.newValue), self = this;

    var replaced = 0, old_val, new_val;
    if (!Array.isArray(this.toRaw(this.current))) this.current = [ this.current ];
    _.each(this.toRaw(this.current), function(document) {
        old_val = clone(document);
        var newDoc = _.defaults(_.clone(self.exec(func, (new Term()).expr(document))), document);
        new_val = clone(newDoc);

        if (!_.isEqual(newDoc, document)) {
            delete table.data[document[table.primaryKey]];
            table.data[newDoc[table.primaryKey]] = newDoc;
            replaced++;
        }
    });

    this.context.inCursor = false;
    var ret = {
        replaced: replaced
    };
    if (options.options && options.options.returnVals) {
        ret.new_val = new_val;
        ret.old_val = old_val;
    }
    return ret;
}

Runner.prototype.replace = function(options) {
    var table = datastore[this.context.db || this.conn.db][this.context.table];

    var func = (new Term()).expr(options.newValue), self = this;

    var replaced = 0, old_val, new_val;
    if (!Array.isArray(this.toRaw(this.current))) this.current = [ this.current ];
    _.each(this.toRaw(this.current), function(document) {
        old_val = clone(document);
        var newDoc = self.exec(func, (new Term()).expr(document));

        newDoc[table.primaryKey] = newDoc[table.primaryKey] || document[table.primaryKey];
        new_val = clone(newDoc);

        if (!_.isEqual(newDoc, document)) {
            delete table.data[document[table.primaryKey]];
            table.data[newDoc[table.primaryKey]] = newDoc;
            replaced++;
        }
    });

    this.context.inCursor = false;
    var ret = {
        replaced: replaced
    };
    if (options.options && options.options.returnVals) {
        ret.new_val = new_val;
        ret.old_val = old_val;
    }
    return ret;
}

Runner.prototype.delete = function(options) {
    var table = datastore[this.context.db || this.conn.db][this.context.table];

    var deleted = 0;
    if (!Array.isArray(this.toRaw(this.current))) this.current = [ this.current ];
    _.each(this.toRaw(this.current), function(document) {
        delete table.data[document[table.primaryKey]];
        deleted++;
    });

    this.context.inCursor = false;
    return {
        deleted: deleted
    };
}
Runner.prototype.sync = function() {
    return { synced: 1 };
}

// Selecting data
Runner.prototype.db = function(options) {
    this.context.db = this.toRaw(options.db);
    if (!this.context.db.match(/^[A-Za-z0-9_]+$/)) throw new Error.ReqlRuntimeError('Database name `' + this.context.db + '` invalid (Use A-Za-z0-9_ only)');
};

Runner.prototype.table = function(options) {
    this.context.table = this.toRaw(options.table);
    this.context.indexes = datastore[this.context.db || this.conn.db][this.context.table].indexes;

    this.context.inCursor = true;
    return _.values(datastore[this.context.db || this.conn.db][this.context.table].data);
};

Runner.prototype.get = function(options) {
    var pk = datastore[this.context.db || this.conn.db][this.context.table].primaryKey;
    
    this.context.inCursor = false;
    return _.find(this.current, function(current) {
        return current[pk] == options.key;
    });
};

Runner.prototype.getAll = function(options) {
    var index = options.keys[options.keys.length - 1].index ? options.keys.pop().index : datastore[this.context.db || this.conn.db][this.context.table].primaryKey;
    var self = this;

    var ret = [];
    _.each(options.keys, function(key) {
        ret = ret.concat(_.filter(self.current, function(curr) {
            return self.evaluateIndex(curr, index, key);
        }));
    })
    return _.uniq(ret);
};

unimplemented('between', false, [ 'start', 'end', 'options?' ]);

Runner.prototype.filter = function(options) {
    var self = this;
    if (typeof options.filter == 'object') {
        return _.filter(this.current, options.filter);
    }
    else {
        return _.filter(this.current, function(curr) {
            return self.exec(options.filter, (new Term()).expr(curr));
        });
    }
}

// Joins
Runner.prototype.innerJoin = function(options) {
    var rightValues = this.toRaw(options.sequence), self = this;
    var ret = [];
    _.each(this.current, function(curr) {
        for (var i = 0; i < rightValues.length; i++) {
            if (self.exec(options.predicate, (new Term()).expr(curr), (new Term()).expr(rightValues[i]))) {
                ret.push({
                    left: curr,
                    right: rightValues[i]
                });
                break;
            }
        }
    });
    return ret;
};
Runner.prototype.outerJoin = function(options) {
    var rightValues = this.toRaw(options.sequence), self = this;
    return _.map(this.current, function(curr) {
        for (var i = 0; i < rightValues.length; i++) {
            if (self.exec(options.predicate, (new Term()).expr(curr), (new Term()).expr(rightValues[i]))) {
                return {
                    left: curr,
                    right: rightValues[i]
                };
            }
        }
        return { left: curr };
    });
};

Runner.prototype.eqJoin = function(options) {
    var rightValues = this.toRaw(options.sequence), self = this;
    var ret = [];
    this.context.inCursor = true;
    _.each(this.current, function(curr) {
        var key = self.exec(options.leftKey, (new Term()).expr(curr));
        for (var i = 0; i < rightValues.length; i++) {
            // super hacky... find a better way
            var runner = (new Runner(self.conn, options.sequence.stack));
            runner.run();
            if (runner.evaluateIndex(rightValues[i], options.predicate ? options.predicate.index : 'id', key)) {
                ret.push({
                    left: curr,
                    right: rightValues[i]
                });
                return;
            }
        }
    });
    return ret;
};

// Transformation
Runner.prototype.map = function(options) {
    var self = this;
    return _.map(this.current, function(val) {
        return self.exec(options.transformation, (new Term()).expr(val));
    });
};
Runner.prototype._inCursor = function(options) {
    this.context.inCursor = true;
    return this.current;
};

Runner.prototype.orderBy = function(options) {
    var self = this, indexSort;
    var fields = options.fields;
    for (var i = 0; i < fields.length; i++) {
        if (fields[i].index) {
            indexSort = fields[i].index;
            fields.splice(i--, 1);
        }
        else if (!(fields[i] instanceof SortOrder)) {
            fields[i] = new SortOrder('asc', fields[i]);
        }
    }
    if (indexSort && !(indexSort instanceof SortOrder)) {
        indexSort = new SortOrder('asc', indexSort);
    }

    return this.current.sort(function(a, b) {
        // Index sorting comes first
        if (indexSort) {
            var ia = self.evaluateIndex(a, indexSort.field);
            var ib = self.evaluateIndex(b, indexSort.field);
            if (ia > ib) return indexSort.direction == 'asc' ? 1 : -1;
            if (ia < ib) return indexSort.direction == 'asc' ? -1 : 1;
            // If equal, fall through
        }
        for (var i = 0; i < fields.length; i++) {
            var ia = self.exec(fields[i].field, a);
            var ib = self.exec(fields[i].field, b);
            if (ia > ib) return fields[i].direction == 'asc' ? 1 : -1;
            if (ia < ib) return fields[i].direction == 'asc' ? -1 : 1;
            // If equal, fall through
        }
        return 0;
    });
};

Runner.prototype.skip = function(options) {
    return this.current.slice(options.value, this.current.length);
};

Runner.prototype.limit = function(options) {
    return this.current.slice(0, options.value);
};

Runner.prototype.slice = function(options) {
    options.options = options.options || {};
    var left = options.options.leftBound && options.options.leftBound == 'open' ? 1 : 0;
    var right = options.options.rightBound && options.options.rightBound == 'closed' ? 1 : 0;

    return this.current.slice(options.start + left, options.end + right);
};
Runner.prototype.nth = function(options) {
    return this.current[options.value];
};
Runner.prototype.indexesOf = function(options) {
    var ret = [];
    for (var i = 0; i < this.current.length; i++) {
        if (this.exec(options.predicate, new Term().expr(this.current[i]))) {
            ret.push(i);
        }
    }
    return ret;
};
Runner.prototype.isEmpty = function(options) {
    this.context.inCursor = false;
    return !this.current.length;
};

Runner.prototype.union = function(options) {
    return this.current.concat(this.toRaw(options.other));
};

Runner.prototype.sample = function(options) {
    if (options.size < 1) throw new Error.ReqlRuntimeError("Number of items to sample must be non-negative, got `" + options.size + "`");

    return _.sample(this.current, options.size);
};


// Aggregations
Runner.prototype.reduce = function(options) {
    if (this.current.length == 0) return undefined;
    while (this.current.length > 1) {
        this.current.push(
            this.exec(
                options.func,
                new Term().expr(this.current.pop()),
                new Term().expr(this.current.pop())
            )
        );
    }
    this.context.inCursor = false;
    return this.current[0];
};

Runner.prototype.count = function(options) {
    var filter = options.filter || function() { return true };
    var total = 0;
    for (var i = 0; i < this.current.length; i++) {
        if (this.exec(filter, new Term().expr(this.current[i]))) {
            total++;
        }
    }
    this.context.inCursor = false;
    return total;
};

unimplemented('distinct', false, []);

Runner.prototype.group = function(options) {
    var elems = [], self = this;
    _.each(this.current, function(item) {
        var currentGroups = _.map(options.groups, function(group) {
            return self.exec(group, (new Term()).expr(item));
        });
        if (currentGroups.length == 1) currentGroups = currentGroups[0];

        var found = false;
        _.each(elems, function(elem) {
            if (_.isEqual(elem.group, currentGroups)) {
                found = true;
                elem.reduction.push(item);
            }
        });
        if (!found) {
            elems.push({
                group: currentGroups,
                reduction: [ item ]
            });
        }
    });

    this.context.inGroup = true;
    this.context.inCursor = true;
    return elems;
};

Runner.prototype.ungroup = function() {
    this.context.inGroup = false;
    return this.current;
};

Runner.prototype.contains = function(options) {
    var self = this;
    this.context.inCursor = false;
    return _(options.args).chain().map(function(arg) {
        if (!(arg instanceof Term) && typeof arg != 'function') {
            arg = (new Term()).row.eq(arg);
        }
        return _(self.current).chain().map(function(item) {
            return self.toRaw(self.exec(arg, (new Term()).expr(item)));
        }).reduce(function(a, b) { return a || b; }, false).value();

    }).reduce(function(a, b) { return a && b; }, true).value();
};

Runner.prototype.sum = function(options) {
    this.context.inCursor = false;
    var args = this.current;
    delete this.current;
    return this.add({ args: args });
};

Runner.prototype.avg = function(options) {
    this.context.inCursor = false;
    var args = this.current;
    delete this.current;
    return this.add({ args: args }) / args.length;
};

Runner.prototype.min = function(options) {
    this.context.inCursor = false;
    return Math.min.apply(Math, this.current);
};

Runner.prototype.max = function(options) {
    this.context.inCursor = false;
    return Math.max.apply(Math, this.current);
};

// Document manipulation
function inArray(data, func) {
    if (Array.isArray(data)) return _.map(data, func);
    return func(data);
}
Runner.prototype.pluck = function(options) {
    var self = this;
    return inArray(this.current, function(curr) {
        return _.pick(curr, self.toRaw(options.fields));
    });
};

Runner.prototype.without = function(options) {
    var self = this;
    return inArray(this.current, function(curr) {
        return _.omit(curr, self.toRaw(options.fields));
    });
};

Runner.prototype.merge = function(options) {
    var self = this;
    return inArray(this.current, function(curr) {
        if (typeof options.arg == 'function') {
        return _.merge(curr, self.exec(options.arg, new Term().expr(curr)));
        }

        return _.merge(curr, self.exec((new Term()).expr(options.arg), new Term().expr(curr)));
    });
};

unimplemented('literal', true, [ 'obj' ]);

Runner.prototype.append = function(options) {
    var a = _.clone(this.current);
    a.push(options.value);
    return a;
};
Runner.prototype.prepend = function(options) {
    var a = _.clone(this.current);
    a.unshift(options.value);
    return a;
};
Runner.prototype.difference = function(options) {
    return _.difference(this.current, this.toRaw(options.other));
};
Runner.prototype.setInsert = function(options) {
    var a = _.clone(this.current);
    a.push(this.toRaw(options.other));

    return _.uniq(a);
};
Runner.prototype.setUnion = function(options) {
    return _.uniq(_.clone(this.current).concat(this.toRaw(options.other)));
};
Runner.prototype.setIntersection = function(options) {
    return _.uniq(_.intersection(this.current, this.toRaw(options.other)));
};
Runner.prototype.setDifference = function(options) {
    return _.uniq(_.difference(this.current, this.toRaw(options.other)));
};

Runner.prototype['(...)'] = function(options) {
    return inArray(this.current, function(curr) {
        return typeof curr[options.field] != 'undefined' ? curr[options.field] : null;
    });
};

Runner.prototype.hasFields = function(options) {
    var self = this;
    if (Array.isArray(this.current)) {
        return _.filter(this.current, function(curr) {
            return _.difference(self.toRaw(options.fields), _.keys(curr)).length == 0;
        });
    }
    else {
        return _.difference(self.toRaw(options.fields), _.keys(this.current)).length == 0;
    }
};

Runner.prototype.insertAt = function(options) {
    var self = this;
    if (Array.isArray(this.current)) {
        return _.filter(this.current, function(curr) {
            return _.difference(self.toRaw(options.fields), _.keys(curr)).length == 0;
        });
    }
    else {
        return _.difference(self.toRaw(options.fields), _.keys(this.current)).length == 0;
    }
};

Runner.prototype.insertAt = function(options) {
    var c = _.clone(this.current);
    c.splice(this.toRaw(options.index), 0, this.toRaw(options.value));
    return c;
};
Runner.prototype.spliceAt = function(options) {
    var a = _.clone(this.toRaw(options.array));
    a.unshift(0);
    a.unshift(this.toRaw(options.index));

    var c = _.clone(this.current);
    c.splice.apply(c, a);
    return c;
};
Runner.prototype.deleteAt = function(options) {
    var c = _.clone(this.current);
    if (this.toRaw(options.end)) c.splice(this.toRaw(options.start), this.toRaw(options.end) - this.toRaw(options.start));
    else c.splice(this.toRaw(options.start), 1);
    return c;
};
Runner.prototype.changeAt = function(options) {
    var c = _.clone(this.current);
    c[this.toRaw(options.index)] = this.toRaw(options.value);
    return c;
};
Runner.prototype.keys = function(options) {
    // TODO: More robust error checking;
    if (typeof this.current == 'string') throw new Error.ReqlRuntimeError('Expected type OBJECT but found STRING.');
    this.context.inCursor = true;
    return _.keys(this.current);
};

Runner.prototype.object = function(options) {
    var args = _.map(options.args, this.toRaw), ret = {};
    for (var i = 1; i < args.length; i += 2) {
        ret[args[i - 1]] = args[i];
    }
    return ret;
};

// String
unimplemented('match', false, [ 'regex' ]);
Runner.prototype.upcase = function() {
    return this.current.toUpperCase();
}
Runner.prototype.downcase = function() {
    return this.current.toLowerCase();
}

// Math and Logic
Runner.prototype.add = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.push(this.current);
    var self = this;
    return _.reduce(args, function(a, b) { return self.toRaw(a) + self.toRaw(b); }, 0);
};
Runner.prototype.sub = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.unshift(this.current);
    var self = this;
    var val = this.toRaw(args.shift());
    while (args.length) val -= this.toRaw(args.shift());
    return val;
};

Runner.prototype.mul = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.push(this.current);
    var self = this;
    return _.reduce(args, function(a, b) { return self.toRaw(a) * self.toRaw(b); }, 1);
};

Runner.prototype.div = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.unshift(this.current);
    var self = this;
    var val = this.toRaw(args.shift());
    while (args.length) val = val / this.toRaw(args.shift());
    return val;
};

Runner.prototype.mod = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.unshift(this.current);
    var self = this;
    return this.toRaw(args.shift()) % this.toRaw(args.shift());
};

Runner.prototype.and = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.push(this.current);
    var self = this;
    return _.reduce(args, function(a, b) { return self.toRaw(a) && self.toRaw(b); }, true);
};

Runner.prototype.or = function(options) {
    var args = _.clone(options.args);
    if (this.current) args.push(this.current);
    var self = this;
    return _.reduce(args, function(a, b) { return self.toRaw(a) || self.toRaw(b); }, false);
};

Runner.prototype.eq = function(options) {
    var args = this.toRaw(options.args);
    if (typeof this.current != 'undefined') args.unshift(this.current);
    var orig = args.shift();
    for (var i = 0; i < args.length; i++) if (!_.isEqual(orig, args[i])) return false;
    return true;
};
Runner.prototype.ne = function(options) {
    return !this.eq(options);
};
function comp(cmp) {
    return function(options) {
        var args = _.clone(this.toRaw(options.args));
        if (typeof this.current != 'undefined') args.unshift(this.current);
        for (var i = 0; i < args.length - 1; i++) {
            if (!cmp(args[i], args[i + 1])) return false;
        }
        return true;
    }
}
Runner.prototype.gt = comp(function(a, b) { return a > b; });
Runner.prototype.ge = comp(function(a, b) { return a >= b; });
Runner.prototype.lt = comp(function(a, b) { return a < b; });
Runner.prototype.le = comp(function(a, b) { return a <= b; });


Runner.prototype.not = function() {
    return !this.current;
}
// Dates and times
Runner.prototype.now = function() {
    return new Date();
};

unimplemented('time', true, []);
unimplemented('epochTime', true, [ 'epochTime' ]);
unimplemented('ISO8601', true, [ 'isoTime', 'options' ]);
unimplemented('inTimezone', false, [ 'timezone' ]);
unimplemented('timezone', false, []);
unimplemented('during', false, [ 'left', 'right', 'options' ]);
unimplemented('date', false, []);
unimplemented('timeOfDay', false, []);
unimplemented('year', false, []);
unimplemented('month', false, []);
unimplemented('day', false, []);
unimplemented('dayOfYear', false, []);
unimplemented('dayOfMonth', false, []);
unimplemented('hours', false, []);
unimplemented('minutes', false, []);
unimplemented('seconds', false, []);
unimplemented('toISO8601', false, []);
unimplemented('toEpochTime', false, []);

// Control structures
Runner.prototype.do = function(options) {
    return this.exec(options.func, (new Term()).expr(this.current));
};

Runner.prototype.branch = function(options) {
    if (this.exec((new Term()).expr(options.predicate), (new Term()).expr(this.current))) {
        return this.exec((new Term()).expr(options.trueBranch));
    }
    else {
        return this.exec((new Term()).expr(options.falseBranch));
    }
};

Runner.prototype.forEach = function(options) {
    this.context.inCursor = false;
    var ret = {}, self = this;
    _.each(this.current, function(item) {
        var r = self.exec(options.func, (new Term()).expr(item));
        _.each(r, function(v, k) {
            ret[k] = ret[k] ? ret[k] + v : v; 
        });
    });
    return ret;
}
unimplemented('error', false, [ 'str' ]);
Runner.prototype.default = function(options) {
    return typeof this.current == 'undefined' || this.current === null ? this.toRaw(options.value) : this.current;
};

Runner.prototype.expr = function(options) {
    var ret = this.toRaw(options.expression);
    if (Array.isArray(ret)) {
        this.context.inCursor = true;
    }
    return ret;
};

Runner.prototype.js = function(options) {
    return eval(options.js);
};

Runner.prototype.coerceTo = function(options) {
    if (this.current instanceof Date) {
        if (options.type == 'number') return +this.current;
        else return this.current.toISOString();
    }
    else if (Array.isArray(this.current)) {
        if (options.type == 'ARRAY') {
            return this.current;
        }
        else {
            var ret = {};
            for (var i = 0; i < this.current.length; i++) {
                ret[this.current[i][0]] = this.current[i][1];
            }
            return ret;
        }
    }
    else if (helper.isPlainObject(this.current)) {
        var ret = [];
        _.forOwn(this.current, function(v, k) {
            ret.push([ k, v ]);
        });
        return ret;
    }
    else if (options.type == 'NUMBER') {
        return Number(this.current);
    }
    else {
        return String(this.current);
    }
};
Runner.prototype.typeOf = function(options) {
    if (Array.isArray(this.current)) return 'ARRAY';
    return (typeof this.current).toUpperCase();
};
Runner.prototype.json = function(options) {
    return JSON.parse(options.json);
};

function s4() {
    return Math.floor((1+Math.random())*0x10000).toString(16).substring(1);
}

function uuid() {
    return s4()+s4()+s4()+s4()+s4()+s4()+s4()+s4();
}
module.exports = Term;
