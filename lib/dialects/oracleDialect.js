var asynk = require('asynk');
var _ = require('underscore');
var shasum = require('crypto').createHash('sha1');
var Errors = require('waterline-errors').adapter;
var SQL = require('../sql.js');

var GenericDialect = require('./genericDialect.js');

var OracleDialect = module.exports = function(){};

OracleDialect.prototype = new GenericDialect();

OracleDialect.prototype.describe = function (client, tableName, callback, LOG_QUERIES) {

    var queries = [],results = [];
    queries[0] = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
    queries[1] = "SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE table_name = '" + tableName + "'";
    queries[2] = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner "
            + "FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '" + tableName
            + "' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner "
            + "ORDER BY cols.table_name, cols.position";

    asynk.each(queries, function(query,nextQuery){
        client.raw(query).then(function(result){
            results[queries.indexOf(query)] = result;
            nextQuery();
        }).catch(function(e){
            nextQuery(e);
        });
    }).args(asynk.item, asynk.callback).serie(function (err) {
        if (err) {
            callback(err,null);
            return;
        }
        var schema = results[0];
        var indexes = results[1];
        var tablePrimaryKeys = results[2];
        if (schema.length === 0) {
            return callback({code : 'ER_NO_SUCH_TABLE', message : 'Table '+tableName+' doesn\'t exist.' },null);
        }
        // Loop through Schema and attach extra attributes
        schema.forEach(function (attribute) {
            tablePrimaryKeys.forEach(function (pk) {
                // Set Primary Key Attribute
                if (attribute.COLUMN_NAME === pk.COLUMN_NAME) {
                    attribute.primaryKey = true;
                    // If also a number set auto increment attribute
                    if (attribute.DATA_TYPE === 'NUMBER') {
                        attribute.autoIncrement = true;
                    }
                }
            });
            // Set Unique Attribute
            if (attribute.NULLABLE === 'N') {
                attribute.required = true;
            }

        });
        // Loop Through Indexes and Add Properties
        indexes.forEach(function (index) {
            schema.forEach(function (attribute) {
                if (attribute.COLUMN_NAME === index.COLUMN_NAME)
                {
                    attribute.indexed = true;
                }
            });
        });
        callback(null, schema);
    }, [null, asynk.data('all')]);
};

OracleDialect.prototype.normalizeSchema =  function(schema,definition) {
        var normalized = _.reduce(schema, function(memo, field) {
           // console.log('definition normalize');console.log(definition);
            // Marshal mysql DESCRIBE to waterline collection semantics
            var attrName = field.COLUMN_NAME.toLowerCase();
            /*Comme oracle n'est pas sensible à la casse, la liste des colonnes retournées est differentes de celle enregistrée dans le schema, 
             ce qui pose des problèmes à waterline*/
            Object.keys(definition).forEach(function (key){
                if(attrName === key.toLowerCase()) attrName = key;
            });
            var type = field.DATA_TYPE;

            // Remove (n) column-size indicators
            type = type.replace(/\([0-9]+\)$/, '');

            memo[attrName] = {
                type: type
                        // defaultsTo: '',
                        //autoIncrement: field.Extra === 'auto_increment'
            };

            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }

            if (field.unique) {
                memo[attrName].unique = field.unique;
            }

            if (field.indexed) {
                memo[attrName].indexed = field.indexed;
            }
            return memo;
        }, {});
        return normalized;
    };


OracleDialect.prototype.escapeString = function(string) {
    if(_.isUndefined(string)) return null;
    return this.stringDelimiter+string+this.stringDelimiter;
};

OracleDialect.prototype.formatIdentifier = function(identifier) {
  if(identifier.length > 30){
    if(!this.identifiers) this.identifiers = {};
    if(!this.identifiers[identifier]){
      shasum.update(identifier);
      this.identifiers[identifier] = shasum.digest('base64');
    }
    return this.identifiers[identifier];
  }
  return identifier;
};

OracleDialect.prototype.beforeFind = function(params, client,cb){
  var options = params.options;
  var connectionObject = params.connectionObject;
  var collection = connectionObject.collections[params.collectionName];
  var attributes = collection.attributes;
  var criteria = SQL.normalizeCriteria(options, attributes);
  /************* surpassing the binary types probleme *******************/
  console.log("dbType: ",connectionObject.config.dbType);
            if(connectionObject.config.dbType === 'oracle'){
            var list = criteria.select || _.keys(collection.definition);
              criteria.select = _.filter(list,function(attr){
                  var type = _.isObject(collection.definition[attr])?collection.definition[attr].type:collection.definition[attr];
                  if(['binary','array','json'].indexOf(type.toLowerCase())<0)
                      return attr;
              });
            }
            console.log(criteria.select);
  /************* ******************** *******************/
  /* replace attributes names by columnNames */
  params.options = criteria;
  params.tableName = this.formatIdentifier(params.collectionName);
  cb(null, params, client);
};

OracleDialect.prototype.beforeCreate = function (params, client, cb) {

  var tableName = this.dialect.formatIdentifier(params.collectionName);
  //var _insertData = lodash.cloneDeep(data);
  var _insertData = _.clone(params.data);

  _.keys(_insertData).forEach(function (key) {
    _insertData[key] = SQL.prepareValue(_insertData[key]);
  });
  var autoInc = null;

  _.keys(params.collection.definition).forEach(function (key) {
    if (_.has(params.collection.definition[key], 'autoIncrement'))
      autoInc = key;

    if (_insertData[key] && !_.isUndefined(params.collection.definition[key].type) && params.collection.definition[key].type === 'binary') {
      _insertData[key] = _insertData[key].toString('hex');
    }
  });
  cb(null, params, client);
};
