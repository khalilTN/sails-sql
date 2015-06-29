var GenericDialect = function() {
};

GenericDialect.prototype.stringDelimiter = "'";

/* Retrieving infos about tables */
GenericDialect.prototype.describe = function(client, tableName,callback, LOG_QUERIES) {
    callback("ERROR: UNDEFINED_METHOD");
};

/* Normalizing schema */
GenericDialect.prototype.normalizeSchema = function(schema) {
    callback("ERROR: UNDEFINED_METHOD");
};

GenericDialect.prototype.escapeString = function(string) {
    callback("ERROR: UNDEFINED_METHOD");
};

GenericDialect.prototype.formatIdentifier = function(identifier) {
    callback("ERROR: UNDEFINED_METHOD");
};

GenericDialect.prototype.makeUnion = function (client, queryObject) {
  callback("ERROR: UNDEFINED_METHOD");
};

GenericDialect.prototype.manyToManyUnion = function (client, queryObject) {
  callback("ERROR: UNDEFINED_METHOD");
};

module.exports = GenericDialect;



