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

GenericDialect.prototype.beforeFind = function(params, client,cb){
  cb(null, params, client);
};

GenericDialect.prototype.afterFind = function(results, params,cb){
  cb(null, results);
};

GenericDialect.prototype.beforeCreate = function(params, client ,cb){
  cb(null, params, client);
};

GenericDialect.prototype.afterCreate = function(results, params ,cb){
  var autoIncData = {};
  if (params.autoInc) {
    autoIncData[params.autoInc] = results[0];
  }
  var values = _.extend({}, params.data, autoIncData);
  cb(null, values);
};

GenericDialect.prototype.beforeUpdate = function(params, client, cb){
  cb(null, params, client);
};

GenericDialect.prototype.afterUpdate = function(results, params, cb){
  cb(null, params.result);
};

GenericDialect.prototype.beforeDestroy = function(params, client, cb){
  cb(null, params, client);
};

GenericDialect.prototype.afterDestroy = function(results, params, cb){
  cb(null, params.result);
};


module.exports = GenericDialect;



