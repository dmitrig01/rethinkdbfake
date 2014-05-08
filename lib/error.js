var helper = require(__dirname+"/helper.js");
var INDENT = 4;
var LIMIT = 80;

function ReqlDriverError(message, query, secondMessage) {
    Error.captureStackTrace(this, ReqlDriverError);
    this.message = message;
    /*
    if ((query != null) && (query._self != null) && (query._self.type != null)) {
        this.message += " after:\n"+generateBacktrace(query._self, 0, null, [-1], {indent: 0, extra: 0}).str;
    }
    else {
        */
        if (this.message[this.message.length-1] !== '?') this.message += ".";
    //}
    if (secondMessage) this.message += "\n"+secondMessage;
};
ReqlDriverError.prototype = new Error();
ReqlDriverError.prototype.name = "ReqlDriverError"; 

module.exports.ReqlDriverError = ReqlDriverError;


function ReqlServerError(message) {
    Error.captureStackTrace(this, ReqlServerError);
    this.message = message;
};
ReqlServerError.prototype = new Error();
ReqlServerError.prototype.name = "ReqlServerError"; 

module.exports.ReqlServerError = ReqlServerError;


function ReqlRuntimeError(message, query, frames) {
    Error.captureStackTrace(this, ReqlRuntimeError);
    this.message = message;

    if ((query) && (frames)) {
        this.message += " in:\n";

        frames = frames.backtrace.frames;
        if (frames) this.frames = frames.slice(0);
        //this.frames = JSON.stringify(frames, null, 2);

        var backtrace = generateBacktrace(query, 0, null, frames, {indent: 0, extra: 0});

        var queryLines = backtrace.str.split('\n');
        var carrotLines = backtrace.car.split('\n');

        for(var i=0; i<queryLines.length; i++) {
            this.message += queryLines[i]+"\n";
            if (carrotLines[i].match(/\^/)) {
                var pos = queryLines[i].match(/[^\s]/);
                if ((pos) && (pos.index)) {
                    this.message += space(pos.index)+carrotLines[i].slice(pos.index)+"\n";
                }
                else {
                    this.message += carrotLines[i]+"\n";
                }
            }
        }
    }
    //this.query = JSON.stringify(query, null, 2);
};
ReqlRuntimeError.prototype = new Error();
ReqlRuntimeError.prototype.name = "ReqlRuntimeError"; 

module.exports.ReqlRuntimeError = ReqlRuntimeError;


function ReqlCompileError(message, query, frames) {
    Error.captureStackTrace(this, ReqlCompileError);
    this.message = message;

    if ((query) && (frames)) {
        this.message += " in:\n";

        frames = frames.backtrace.frames;
        if (frames) this.frames = frames.slice(0);
        //this.frames = JSON.stringify(frames, null, 2);

        var backtrace = generateBacktrace(query, 0, null, frames, {indent: 0, extra: 0});

        var queryLines = backtrace.str.split('\n');
        var carrotLines = backtrace.car.split('\n');

        for(var i=0; i<queryLines.length; i++) {
            this.message += queryLines[i]+"\n";
            if (carrotLines[i].match(/\^/)) {
                var pos = queryLines[i].match(/[^\s]/);
                if ((pos) && (pos.index)) {
                    this.message += space(pos.index)+carrotLines[i].slice(pos.index)+"\n";
                }
                else {
                    this.message += carrotLines[i]+"\n";
                }
            }
        }
    }
};
ReqlCompileError.prototype = new Error();
ReqlCompileError.prototype.name = "ReqlCompileError"; 

module.exports.ReqlCompileError = ReqlCompileError;


function ReqlClientError(message) {
    Error.captureStackTrace(this, ReqlClientError);
    this.message = message;
};
ReqlClientError.prototype = new Error();
ReqlClientError.prototype.name = "ReqlClientError"; 

module.exports.ReqlClientError = ReqlClientError;




nonPrefix = {
    DB: true,
    DB_CREATE: true,
    DB_LIST: true,
    DB_DROP: true,
    JS: true,
    NOW: true,
    TIME: true,
    EPOCH_TIME: true,
    ISO8601: true,
    BRANCH: true,
    JAVASCRIPT: true,
    ERROR: true,
    MAKE_ARRAY: true,
    MAKE_OBJ: true,
    MAKE_ARRAY: true,
    JSON: true
}

typeToString = {
    DB: "db",
    DB_CREATE: "dbCreate",
    DB_LIST: "dbList",
    DB_DROP: "dbDrop",
    TABLE_CREATE: "tableCreate",
    TABLE_LIST: "tableList",
    TABLE_DROP: "tableDrop",
    TABLE: "table",
    INDEX_CREATE: "indexCreate",
    INDEX_DROP: "indexDrop",
    INDEX_LIST: "indexList",
    INDEX_WAIT: "indexWait",
    INDEX_STATUS: "indexStatus",
    INSERT: "insert",
    UPDATE: "update",
    REPLACE: "replace",
    DELETE: "delete",
    SYNC: "sync",
    GET: "get",
    GET_ALL: "getAll",
    BETWEEN: "between",
    FILTER: "filter",
    INNER_JOIN: "innerJoin",
    OUTER_JOIN: "outerJoin",
    EQ_JOIN: "eqJoin",
    ZIP: "zip",
    MAP: "map",
    WITH_FIELDS: "withFields",
    CONCATMAP: "concatMap",
    ORDERBY: "orderBy",
    SKIP: "skip",
    LIMIT: "limit",
    SLICE: "slice",
    NTH: "nth",
    INDEXES_OF: "indexes_of",
    IS_EMPTY: "isEmpty",
    UNION: "union",
    SAMPLE: "sample",
    REDUCE: "reduce",
    COUNT: "count",
    SUM: "sum",
    AVG: "avg",
    MIN: "min",
    MAX: "max",
    OBJECT: "object",
    DISTINCT: "distinct",
    GROUP: "group",
    UNGROUP: "ungroup",
    CONTAINS: "contains",
    IMPLICIT_VAR: "row",
    PLUCK: "pluck",
    WITHOUT: "without",
    MERGE: "merge",
    APPEND: "append",
    PREPEND: "prepend",
    DIFFERENCE: "difference",
    SET_INSERT: "setInsert",
    SET_UNION: "setUnion",
    SET_INTERSECTION: "setIntersection",
    SET_DIFFERENCE: "setDifference",
    HAS_FIELDS: "hasFields",
    INSERT_AT: "insertAt",
    SPLICE_AT: "spliceAt",
    DELETE_AT: "deleteAt",
    CHANGE_AT: "changeAt",
    KEYS: "keys",
    MATCH: "match",
    UPCASE: "upcase",
    DOWNCASE: "downcase",
    ADD: "add",
    SUB: "sub",
    MUL: "mul",
    DIV: "div",
    MOD: "mod",
    ALL: "and",
    ANY: "or",
    EQ: "eq",
    NE: "ne",
    GT: "gt",
    GE: "ge",
    LT: "lt",
    LE: "le",
    NOT: "not",
    NOW: "now",
    TIME: "time",
    EPOCH_TIME: "epochTime",
    ISO8601: "ISO8601",
    IN_TIMEZONE: "inTimezone",
    TIMEZONE: "timezone",
    DURING: "during",
    DATE: "date",
    TIME_OF_DAY: "timeOfDay",
    YEAR: "year",
    MONTH: "month",
    DAY: "day",
    DAY_OF_WEEK: "dayOfWeek",
    DAY_OF_YEAR: "dayOfYear",
    HOURS: "hours",
    MINUTES: "minutes",
    SECONDS: "seconds",
    TO_ISO8601: "toISO8601",
    TO_EPOCH_TIME: "toEpochTime",
    FUNCALL: "do",
    BRANCH: "branch",
    FOREACH: "forEach",
    ERROR: "error",
    DEFAULT: "default",
    JAVASCRIPT: "js",
    COERCE_TO: "coerceTo",
    TYPEOF: "typeOf",
    INFO: "info",
    JSON: "json"
}

specialType = {
    DATUM: function(term, index, father, frames, options) {
        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        var result = {
            str: "",
            car: ""
        }
        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) carify(result, "r.expr(", underline)

        if (term.datum.type === "R_NUM") {
            carify(result, ""+term.datum.r_num, underline);
        }
        else if (term.datum.type === "R_STR") {
            carify(result, '"'+term.datum.r_str+'"', underline);
        }
        else if (term.datum.type === "R_BOOL") {
            carify(result, ""+term.datum.r_bool, underline);
        }
        else if (term.datum.type === "R_NULL") {
            carify(result, "null", underline);
        }


        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) carify(result, ")", underline);

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    TABLE: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        }
        var backtrace, underline, currentFrame;


        if ((term.args.length === 0) || (term.args[0].type !== "DB")) {
            underline = Array.isArray(frames) && (frames.length === 0);
            if (Array.isArray(frames)) currentFrame = frames.shift();

            carify(result, "r."+typeToString[term.type]+"(", underline);
            if (term.args) {
                for(var i=0; i<term.args.length; i++) {
                    if (i !==0) result.str += ", ";


                    if ((currentFrame != null) && (currentFrame.pos === 1)) {
                        // +1 for index because it's like if there was a r.db(...) before .table(...)
                        backtrace = generateBacktrace(term.args[i], i+1, term, frames, options)
                    }
                    else {
                        backtrace = generateBacktrace(term.args[i], i+1, term, null, options)
                    }
                    result.str += backtrace.str;
                    result.car += backtrace.car
                }
            }

            backtrace = makeOptargs(term, i, term, frames, options, currentFrame)
            result.str += backtrace.str;
            result.car += backtrace.car;

            carify(result, ")", underline);

            if (underline) result.car = result.str.replace(/./g, '^');
        }
        else {
            backtrace = generateNormalBacktrace(term, index, father, frames, options);
            result.str = backtrace.str;
            result.car = backtrace.car;
        }

        return result;
    },
    GET_FIELD: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        }
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((currentFrame != null) && (currentFrame.pos === 0)) {
            backtrace = generateBacktrace(term.args[0], 0, term, frames, options)
        }
        else {
            backtrace = generateBacktrace(term.args[0], 0, term, null, options)
        }
        result.str = backtrace.str;
        result.car = backtrace.car;

        carify(result, "(", underline);

        if ((currentFrame != null) && (currentFrame.pos === 1)) {
            backtrace = generateBacktrace(term.args[1], 1, term, frames, options)
        }
        else {
            backtrace = generateBacktrace(term.args[1], 1, term, null, options)
        }
        result.str += backtrace.str;
        result.car += backtrace.car;

        carify(result, ")", underline);

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    MAKE_ARRAY: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        };
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) carify(result, "r.expr(", underline)

        if (!((options) && (options.noBracket))) {
            carify(result, "[", underline);
        }
        for(var i=0; i<term.args.length; i++) {
            if (i !== 0) {
                carify(result, ", ", underline);
            }

            if ((currentFrame != null) && (currentFrame.pos  === i)) {
                backtrace = generateBacktrace(term.args[i], i, term, frames, options);
            }
            else {
                backtrace = generateBacktrace(term.args[i], i, term, null, options);
            }
            result.str += backtrace.str;
            result.car += backtrace.car;

        }

        if (!((options) && (options.noBracket))) {
            carify(result, "]", underline);
        }

        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) {
            carify(result, ")", underline);
        }

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    MAKE_OBJ: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        }
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) carify(result, "r.expr(", underline)

        if (term.optargs.length === 0) {
            carify(result, "{", underline);
        }
        else {
            carify(result, "{\n", underline);
        }
        options.indent += INDENT+options.extra;
        var extraToRemove = options.extra;
        if (!((father) && (Array.isArray(father.optargs)) && (father.optargs.length > 0))) options.extra = 0;

        for(var i=0; i<term.optargs.length; i++) {
            if (i !== 0) carify(result, ",\n", underline);

            carify(result, space(options.indent)+term.optargs[i].key+": ", underline || ((currentFrame != null) && (currentFrame.opt === term.optargs[i].key)));


            if ((currentFrame != null) && (currentFrame.opt === term.optargs[i].key)) {
                backtrace = generateBacktrace(term.optargs[i].val, index, term, frames, options);
            }
            else {
                backtrace = generateBacktrace(term.optargs[i].val, index, term, null, options);
            }
            result.str += backtrace.str;
            result.car += backtrace.car;
        }
        options.indent -= INDENT+extraToRemove;

        if (term.optargs.length === 0) {
            carify(result, "}", underline);
        }
        else {
            carify(result, "\n"+space(options.indent+extraToRemove)+"}", underline);
        }


        if ((index === 0) && ((father == null) || (!nonPrefix[father.type]))) carify(result, ")", underline);

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    FUNC: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        };
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((term.args[0].args.length === 1) && (helper.hasImplicit(term.args[1]))) {
            if ((currentFrame != null) && (currentFrame.pos === 1)) {
                backtrace = generateBacktrace(term.args[1], 1, term, frames, options);
            }
            else {
                backtrace = generateBacktrace(term.args[1], 1, term, null, options);
            }
            result.str = backtrace.str;
            result.car = backtrace.car;
        }
        else {
            carify(result, "function(", underline);

            for(var i=0; i<term.args[0].args.length; i++) {
                if (i !== 0) {
                    carify(result, ", ", underline);
                }
                carify(result, "var_"+term.args[0].args[i].datum.r_num, underline);
            }

            options.indent += INDENT+options.extra;
            var extraToRemove = options.extra;
            if (!((Array.isArray(term.optargs)) && (term.optargs.length > 0))) options.extra = 0;

            carify(result, ") {\n"+space(options.indent)+"return ", underline);

            if ((currentFrame != null) && (currentFrame.pos === 1)) {
                backtrace = generateBacktrace(term.args[1], 1, term, frames, options);
            }
            else {
                backtrace = generateBacktrace(term.args[1], 1, term, null, options);
            }

            result.str += backtrace.str;
            result.car += backtrace.car;

            options.indent -= INDENT+extraToRemove;

            carify(result, "\n"+space(options.indent+extraToRemove)+"}", underline);

        }

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    VAR: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        }
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        carify(result, "var_"+term.args[0].datum.r_num, underline);

        if (underline) result.car = result.str.replace(/./g, '^');
        return result;
    },
    FUNCALL: function(term, index, father, frames, options) {
        // The syntax is args[1].do(args[0])
        var result = {
            str: "",
            car: ""
        };
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((currentFrame != null) && (currentFrame.pos === 1)) {
            backtrace = generateBacktrace(term.args[1], 0, term, frames, options);
        }
        else {
            backtrace = generateBacktrace(term.args[1], 0, term, null, options);
        }
        result.str = backtrace.str;
        result.car = backtrace.car;

        carify(result, ".do(", underline);

        if ((currentFrame != null) && (currentFrame.pos === 0)) {
            backtrace = generateBacktrace(term.args[0], 0, term, frames, options);
        }
        else {
            backtrace = generateBacktrace(term.args[0], 0, term, null, options);
        }
        result.str += backtrace.str;
        result.car += backtrace.car;

        carify(result, ")", underline);

        if (underline) result.car = result.str.replace(/./g, '^');

        return result;
    },
    IMPLICIT_VAR: function(term, index, father, frames, options) {
        var result = {
            str: "",
            car: ""
        }
        var backtrace, underline, currentFrame;

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        carify(result, "r.row", underline);

        if (underline) result.car = result.str.replace(/./g, '^');
        return result;
    }
}
specialType.TABLE_CREATE = specialType.TABLE;
specialType.TABLE_DROP = specialType.TABLE;
specialType.TABLE_LIST = specialType.TABLE;

function space(n) {
    return new Array(n+1).join(' ');
}
function carify(result, str, underline) {
    if (underline === true) {
        result.str += str;
        result.car += str.replace(/[^\n]/g, '^');
    }
    else {
        result.str += str;
        result.car += str.replace(/[^\n]/g, ' ');
    }
}
function makeOptargs(term, index, father, frames, options, currentFrame) {
    var result = {
        str: "",
        car: ""
    }
    var backtrace, currentFrame, underline;

    if ((Array.isArray(term.optargs)) && (term.optargs.length > 0)) {
        if ((currentFrame != null) && (frames != null)) frames.unshift(currentFrame);

        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        carify(result, ", " , underline);

        var optargs = {
            type: "MAKE_OBJ",
            optargs: term.optargs
        }
        backtrace = specialType.MAKE_OBJ(optargs, index, term, frames, options);

        result.str += backtrace.str;
        result.car += backtrace.car;

        if (underline) result.car = result.str.replace(/./g, '^');
    }

    return result;
}
function generateNormalBacktrace(term, index, father, frames, options) {
    var result = {
        str: "",
        car: ""
    }
    var backtrace, currentFrame, underline;

    if (term.args) {
        underline = Array.isArray(frames) && (frames.length === 0);
        if (Array.isArray(frames)) currentFrame = frames.shift();

        if ((currentFrame != null) && (currentFrame.pos === 0)) {
            backtrace = generateBacktrace(term.args[0], 0, term, frames, options);
        }
        else {
            backtrace = generateBacktrace(term.args[0], 0, term, null, options);
        }
        result.str = backtrace.str;
        result.car = backtrace.car;

        var lines = backtrace.str.split('\n');
        var line = lines[lines.length-1];
        var pos = line.match(/[^\s]/);
        pos = (pos) ? pos.index : 0;

        if (line.length-pos > LIMIT) {
            if (options.extra === 0) options.extra += INDENT;
            carify(result, "\n"+space(options.indent+options.extra) , underline);
        }

        carify(result, "."+typeToString[term.type]+"(" , underline);

        for(var i=1; i<term.args.length; i++) {
            if (i !== 1) {
                carify(result, ", " , underline);
            }
            if ((currentFrame != null) && (currentFrame.pos === i)) {
                backtrace = generateBacktrace(term.args[i], i, term, frames, options);
            }
            else {
                backtrace = generateBacktrace(term.args[i], i, term, null, options);
            }
            result.str += backtrace.str;
            result.car += backtrace.car;
        }

        backtrace = makeOptargs(term, i, term, frames, options, currentFrame)
        result.str += backtrace.str;
        result.car += backtrace.car;


        carify(result, ")" , underline);

        if (underline) result.car = result.str.replace(/./g, '^');
    }
    else {
        throw new Error("The driver should never enter this condition. Please report the query to the developers -- End 1 --\n"+JSON.stringify(term, null, 2))
    }


    return result;
}

function generateWithoutPrefixBacktrace(term, index, father, frames, options) {
    var result = {
        str: "",
        car: ""
    }

    var backtrace, currentFrame, underline;

    underline = Array.isArray(frames) && (frames.length === 0);
    if (Array.isArray(frames)) currentFrame = frames.shift();

    carify(result, "r."+typeToString[term.type]+"(", underline); 

    if (term.args) {
        for(var i=0; i<term.args.length; i++) {
            if (i !==0) result.str += ", ";

            if ((currentFrame != null) && (currentFrame.pos === i)) {
                backtrace = generateBacktrace(term.args[i], i, term, frames, options)
            }
            else {
                backtrace = generateBacktrace(term.args[i], i, term, null, options)
            }
            result.str += backtrace.str;
            result.car += backtrace.car;
        }
    }
    carify(result, ")", underline); 

    if (underline) result.car = result.str.replace(/./g, '^');

    return result;
}

function generateBacktrace(term, index, father, frames, options) {
    var result = {
        str: "",
        car: ""
    }
    var backtrace, currentFrame, underline;

    // frames = null -> do not underline
    // frames = [] -> underline

    if (term) {
        if (specialType[term.type]) {
            backtrace = specialType[term.type](term, index, father, frames, options);
            result.str = backtrace.str;
            result.car = backtrace.car;
        }
        else if (nonPrefix[term.type]) {
            backtrace = generateWithoutPrefixBacktrace(term, index, father, frames, options);
            result.str = backtrace.str;
            result.car = backtrace.car;
        }
        else { // normal type -- this.<method>( this.args... )
            backtrace = generateNormalBacktrace(term, index, father, frames, options);
            result.str = backtrace.str;
            result.car = backtrace.car;
        }
    }
    else {
        //throw new Error("The driver should never enter this condition. Please report the query to the developers -- End 2")
    }
    return result;
}
module.exports.generateBacktrace = generateBacktrace;
