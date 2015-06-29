var asynk = require('asynk');
var _ = require('underscore');
var SQL = require('../sql.js');

var GenericDialect = require('./genericDialect.js');

var MysqlDialect = module.exports = function(){};

MysqlDialect.prototype = new GenericDialect();

MysqlDialect.prototype.describe = function (client, tableName, callback, LOG_QUERIES) {
    var query = 'DESCRIBE ' + tableName;
    var pkQuery = 'SHOW INDEX FROM ' + tableName;
    
    if (LOG_QUERIES) {
        console.log('\nExecuting MySQL query :', query);
        console.log('Executing MySQL query :', pkQuery);
    }
    client.raw(query).then(function __DESCRIBE__(result) {
        client.raw(pkQuery).then(function (pkResult) {
            var schema = result[0];
            schema.forEach(function (attr) {

                if (attr.Key === 'PRI') {
                    attr.primaryKey = true;

                    if (attr.Type === 'int(11)') {
                        attr.autoIncrement = true;
                    }
                }

                if (attr.Key === 'UNI') {
                    attr.unique = true;
                }
            });

            pkResult.forEach(function (result) {
                schema.forEach(function (attr) {
                    if (attr.Field !== result.Column_name)
                        return;
                    attr.indexed = true;
                });
            });
            callback(null,schema);
        });
    }).catch(function (e) {
            callback(e,null);
    });
};

MysqlDialect.prototype.normalizeSchema = function (schema) {
        var normalized = _.reduce(schema, function (memo, field) {

            var attrName = field.Field;
            var type = field.Type;

            type = type.replace(/\([0-9]+\)$/, '');
            memo[attrName] = {
                type: type,
                defaultsTo: field.Default,
                autoIncrement: field.Extra === 'autoIncrement'
            };
            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }
            if (field.autoIncrement) {
                memo[attrName].autoIncrement = field.autoIncrement;
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
    
MysqlDialect.prototype.escapeString = function(string) {
    if(_.isUndefined(string)) return null;
    return this.stringDelimiter+string+this.stringDelimiter;
};

MysqlDialect.prototype.formatIdentifier = function(identifier) {
    return identifier;
};


MysqlDialect.prototype.makeUnion = function (client, queryObject) {
        if (!queryObject || queryObject.criteriasByParent.length === 0)
            return null;
        var unionQuery = client.select('*').from(function () {
            var from = this;
            queryObject.criteriasByParent.forEach(function (criteria,index) {
                from.union(function(){
                    SQL.select(this, queryObject.collectionName, queryObject.schema, criteria, true);
                },true);
                if(index === queryObject.criteriasByParent.length -1)
                    from.as('union');
            });
        });
        return unionQuery;
    };
    
MysqlDialect.prototype.manyToManyUnion = function(client,queryObject){
        if (!queryObject || queryObject.parentIds.length === 0)
            return null;
        if(!queryObject.criteria.criteria.select) queryObject.criteria.criteria.select = [];
        queryObject.criteria.criteria.select.push(queryObject.childCollection+'.*');
        queryObject.criteria.criteria.select.push(queryObject.jonctionCollection+'.'+queryObject.jonctionParentFK+' as ___'+queryObject.jonctionParentFK);
        if(!queryObject.criteria.criteria.sort){
            queryObject.criteria.criteria.sort = {};
            queryObject.criteria.criteria.sort[queryObject.childPK] = 1;
        }
        var unionQuery = client.select('*').from(function () {
            var from = this;
            queryObject.parentIds.forEach(function(id, index){
                from.union(function(){
                    var req = SQL.select(this, queryObject.childCollection,queryObject.schema,queryObject.criteria.criteria, true);
                    req = req.innerJoin(queryObject.jonctionCollection,queryObject.jonctionCollection+'.'+queryObject.jonctionChildFK,queryObject.childCollection+'.'+queryObject.childPK);
                    req.whereIn(queryObject.childCollection+'.'+queryObject.childPK,function(){
                        this.select(queryObject.jonctionCollection+'.'+queryObject.jonctionChildFK).from(queryObject.jonctionCollection).where(queryObject.jonctionCollection+'.'+queryObject.jonctionParentFK,id);
                    });
                },true);
                if(index === queryObject.parentIds.length -1)
                    from.as('union');
            });
        });
        return unionQuery;
    };
