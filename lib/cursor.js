var Promise = require("bluebird");

function Cursor(data) {
    this.data = data;
}

Cursor.prototype.toJSON = function() {
    return "You cannot serialize to JSON a cursor. Retrieve data from the cursor with `toArray` or `next`."
}

Cursor.prototype.next = function() {
    return Promise.resolve(this.data.shift());
}
Cursor.prototype.hasNext = function() {
    // If we are fetching data, there is more, or if there are documents left
    return !!this.data.length;
}
Cursor.prototype.toArray = function() {
    return Promise.resolve(this.data);
}

Cursor.prototype.close = function() {
    return Promise.resolve();
}

module.exports = Cursor;
