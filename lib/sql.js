var Knex = require('knex');
var _ = require('underscore');
var WhereProcessor = require('./whereProcessor');
var sql = {
    setSchema: function(schema){
      this.schema = schema;
    },
    defineColumn: function (table, attrName, attribute) {
        var column;
        if (attribute.autoIncrement)
            table.increments(attrName);
        else {
            switch (attribute.type) {// defining type
                case 'string':
                    column = table.string(attrName, attribute.size || undefined);
                    break;
                case 'text':
                    column = table.text(attrName);
                    break;
                case 'mediumtext':
                    column = table.text(attrName, 'mediumtext');
                    break;
                case 'array':
                case 'json':
                case 'longtext':
                    column = table.text(attrName, 'longtext');
                    break;
                case 'binary':
                    column = table.binary(attrName);
                    break;
                case 'boolean':
                    column = table.boolean(attrName);
                    break;
                case 'datetime':
                    column = table.datetime(attrName);
                    break;
                case 'date':
                    column = table.datetime(attrName);
                    break;
                case 'time':
                    column = table.time(attrName);
                    break;
                case 'float':
                case 'double':
                    column = table.float(attrName, 16, 8);
                    break;
                case 'decimal':
                    column = table.decimal(attrName);
                    break;
                case 'int':
                case 'integer':
                    column = table.integer(attrName);
                    break;
                default:
                    console.error("Unregistered type given: '" + attribute.type + "', TEXT type will be used");
                    return "TEXT";
            }
        }
        if (attribute.primaryKey)
            column.primary();

        
        else if (attribute.unique)
            column.unique();

        if (attribute.required || attribute.notNull)
            column.notNullable();

        if (attribute.index)
            column.index();

        return column;
    },
    normalizeSchema: function (schema) {
        return _.reduce(schema, function (memo, field) {

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
    },
    select: function (client, collectionName, options, schema) {
        
        var joins = this.processOneToOnePopulations(collectionName,options, schema);
      
        var query = client.select(options.select).from(collectionName);

        if (options.where)
            query = new WhereProcessor(query, collectionName, options.where).process();
        if (options.sum)
            options.sum.forEach(function (keyToSum) {
                query = query.sum(keyToSum + ' as ' + keyToSum);
            });
        if (options.average)
            options.average.forEach(function (keyToAvg) {
                query = query.avg(keyToAvg + ' as ' + keyToAvg);
            });
        if (options.min)
            options.min.forEach(function (keyToMin) {
                query = query.min(keyToMin + ' as ' + keyToMin);
            });
        if (options.max)
            options.max.forEach(function (keyToMax) {
                query = query.max(keyToMax + ' as ' + keyToMax);
            });

        if (options.groupBy)
            options.groupBy.forEach(function (groupKey) {
                query = query.groupBy(groupKey);
            });

        if (options.limit)
            query = query.limit(options.limit);

        if (options.skip)
            query = query.offset(options.skip);

        if (options.sort)
            _.keys(options.sort).forEach(function (toSort) {
                var direction = options.sort[toSort] === 1 ? 'ASC' : 'DESC';
                query = query.orderBy(collectionName+'.'+toSort, direction);
            });
        if(joins){
          joins.forEach(function(join){
            query.leftOuterJoin(join.childTable, join.parentKey, '=', join.childKey);
          });
        }
        return query;
    },
    normalizeCriteria: function (criteria, attributes) {
        var _criteria = _.clone(criteria);
        if (_criteria.select) {
            _criteria.select = _.map(_criteria.select, function (attr) {
                return attributes[attr].columnName || attr;
            });
        }
        //this.normalizeWhere(_criteria.where, attributes);
        this.normalizeAgregates(_criteria, attributes);
        //this.normalizeSort(_criteria.sort, attributes);
        return _criteria;
        //return criteria;
    },
    dropTable: function (client, collectionName) {
        return client.schema.dropTableIfExists(collectionName);
    },
    destroy: function (client, collectionName, options) {
        var deleteQuery = client(collectionName);
        if (options.where)
            deleteQuery = new WhereProcessor(deleteQuery, collectionName, options.where).process();
        return deleteQuery.del();
    },
    insert: function (client, collectionName, record) {
        return client(collectionName).insert(record);
    },
    update: function (client, collectionName, options, data) {
        var updateQuery = client(collectionName);
        if (options.where)
            updateQuery = new WhereProcessor(updateQuery, collectionName, options.where).process();
        return updateQuery.update(data);
    },
    prepareValue: function (value) {
        if (_.isUndefined(value) || value === null)
            return value;

        // Cast functions to strings
        if (_.isFunction(value)) {
            value = value.toString();
        }

        // Store Arrays and Objects as strings
        if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
            try {
                value = JSON.stringify(value);
            } catch (e) {
                // just keep the value and let the db handle an error
                value = value;
            }
        }


        return value;
    },
    normalizeWhere: function (where, attributes) {
        if (!where)
            return;
        var self = this;
        var whereKeys = _.keys(where);
        whereKeys.forEach(function (columnName) {
            if (_.isObject(where[columnName]))
                self.normalizeWhere(where[columnName], attributes);
        });
    },
    normalizeAgregates: function (criteria, attributes) {
        if (!criteria.sum && !criteria.average && !criteria.min && !criteria.max) {
            return;
        }
        criteria.isThereAgreagtes = true;

        criteria.select = [];
        if (criteria.groupBy) {
            if (!_.isArray(criteria.groupBy))
                criteria.groupBy = [criteria.groupBy];
            criteria.select = criteria.select.concat(criteria.groupBy);
        }

        if (criteria.sum) {
            if (!_.isArray(criteria.sum))
                criteria.sum = [criteria.sum];
            criteria.sum.forEach(function (key) {
                var i = criteria.sum.indexOf(key);
                criteria.sum[i] = key;
            });
        }
        if (criteria.average) {
            if (!_.isArray(criteria.average))
                criteria.average = [criteria.average];
            criteria.average.forEach(function (key) {
                var i = criteria.average.indexOf(key);
                criteria.average[i] = key;
            });
        }
        if (criteria.max) {
            if (!_.isArray(criteria.max))
                criteria.max = [criteria.max];
            criteria.max.forEach(function (key) {
                var i = criteria.max.indexOf(key);
                criteria.max[i] = key;
            });
        }
        if (criteria.min) {
            if (!_.isArray(criteria.min))
                criteria.min = [criteria.min];
            criteria.min.forEach(function (key) {
                var i = criteria.min.indexOf(key);
                criteria.min[i] = key;
            });
        }
    },
    normalizeSort: function (sort, attributes) {
    },
    processOneToOnePopulations: function (parent ,criteria, schema) {
        var self = this;
        if (!criteria.instructions)
            return null;
        if (criteria.select){
          criteria.select = _.map(criteria.select , function(column){
            return parent+'.'+column;
          });
        }else{
          criteria.select = [];
          var definition = self.getDefinitionBytableName(schema, parent).attributes;
          _.keys(definition).forEach(function (attributeName) {
            if (definition[attributeName].collection)
              return;
            var columnName = definition[attributeName].columnName || attributeName;
            criteria.select.push(parent + '.' + columnName);
          });
        }
        var joins = [];
        _.keys(criteria.instructions).forEach(function (population) {
            var populationObject = criteria.instructions[population];
            if (populationObject.strategy.strategy === 1) {
                var infos = populationObject.instructions[0];
                var childTableAlias = '_' + infos.alias;
                joins.push({childTable : infos.child + ' as ' + childTableAlias, parentKey : infos.parent + '.' + infos.parentKey, childKey : childTableAlias + '.' + infos.childKey});
                
                var childDefinition = self.getDefinitionBytableName(schema, infos.child).attributes;
                
                var childColumnAliasPrefix = infos.parentKeyAlias || infos.parentKey;
                _.keys(childDefinition).forEach(function (attributeName) {
                    if (childDefinition[attributeName].collection)
                        return;
                    var columnName = childDefinition[attributeName].columnName || attributeName;
                    var childColumnAlias = childColumnAliasPrefix + '___' + columnName;
                    criteria.select.push(childTableAlias + '.' + columnName + ' as ' + childColumnAlias);
                });
            }
        });
        return joins;
    },
    getDefinitionBytableName: function (schema, tableName) {
        var self = this;
        return schema[_.find(_.keys(schema), function (collection) {
            return schema[collection].tableName === tableName;
        })];
    },
    splitStrategyOneChildren: function (parent, mapping) {
        var splitedChild = {};
        _.keys(parent).forEach(function (key) {
            // Check if we can split this on our special alias identifier '___' and if
            // so put the result in the cache
            var split = key.split('___');
            if (split.length < 2)
                return;
            var parentKey = mapping[split[0]];
            if (!_.has(splitedChild, parentKey))
                splitedChild[parentKey] = {};
            splitedChild[parentKey][split[1]] = parent[key];
            delete parent[key];
        });
        if (_.keys(splitedChild).length > 0)
            return splitedChild;
        return null;
    },
                            
    fetchOneToManyChildren: function (client, queryObject) {
      if (!queryObject || queryObject.parentsIds.length === 0)
        return null;
      var self = this;
      var criteria = queryObject.criteria;
      var sort;
      if(criteria.sort){
        criteria.select = _.map(criteria.select || queryObject.childColumns, function(column){
            return queryObject.childCollection+'.'+column;
        });
        _.keys(criteria.sort).forEach(function(key){
          if(criteria.select.indexOf(queryObject.childCollection+'.'+key) === -1)
            criteria.select.push(queryObject.childCollection+'.'+key);
        });
        sort = criteria.sort;
        if(!criteria.limit && !criteria.skip) delete criteria.sort;
      }
      var unionQuery;
      queryObject.parentsIds.forEach(function (id) {
        var criteria = _.clone(queryObject.criteria);
        criteria.where = _.clone(queryObject.criteria.where);
        criteria.where[queryObject.foreignKey] = id;
        if(!unionQuery){
          unionQuery = client.union(function(){
            self.select(this, queryObject.childCollection, criteria);
          }, true);
        }
        else{
          unionQuery.union(function(){
            self.select(this, queryObject.childCollection, criteria);
          }, true);
        }
      });
      _.keys(sort).forEach(function(key){
        unionQuery.orderByRaw((criteria.select.indexOf(queryObject.childCollection+'.'+key) + 1) + ' ' +(sort[key] === 1 ? 'ASC' : 'DESC'));
      });
      return unionQuery;
    },
    
    fetchManyToManyChildren: function (client, queryObject) {
      if (!queryObject || queryObject.parentsIds.length === 0)
        return null;
      var self = this;
      var criteria = queryObject.criteria;
      criteria.select = _.map(criteria.select || queryObject.childColumns, function(column){
        return queryObject.childCollection+'.'+column;
      });
      criteria.select.push(queryObject.jonctionCollection + '.' + queryObject.jonctionParentFK + ' as ___' + queryObject.jonctionParentFK);
      if (!criteria.sort) {
        criteria.sort = {};
        criteria.sort[queryObject.childPK] = 1;
      }
      _.keys(criteria.sort).forEach(function(key){
        if(criteria.select.indexOf(queryObject.childCollection+'.'+key) === -1)
          criteria.select.push(queryObject.childCollection+'.'+key);
      });
      var sort = criteria.sort;
      if(!criteria.limit && !criteria.skip) delete criteria.sort;
      
      function query(context, id){
        var req = self.select(context, queryObject.childCollection, criteria);
        req.innerJoin(queryObject.jonctionCollection, queryObject.jonctionCollection + '.' + queryObject.jonctionChildFK, queryObject.childCollection + '.' + queryObject.childPK);
        req.whereIn(queryObject.childCollection + '.' + queryObject.childPK, function () {
          this.select(queryObject.jonctionCollection + '.' + queryObject.jonctionChildFK).from(queryObject.jonctionCollection).where(queryObject.jonctionCollection + '.' + queryObject.jonctionParentFK, id);
        });
        req.where(queryObject.jonctionCollection + '.' + queryObject.jonctionParentFK, id);
      }
      
      var unionQuery;
      queryObject.parentsIds.forEach(function (id) {
        if (!unionQuery) {
          unionQuery = client.union(function () {
            query(this, id);
          }, true);
        }
        else {
          unionQuery.union(function () {
            query(this, id);
          }, true);
        }
      });
      _.keys(sort).forEach(function(key){
        unionQuery.orderByRaw((criteria.select.indexOf(queryObject.childCollection+'.'+key) + 1) + ' ' +(sort[key] === 1 ? 'ASC' : 'DESC'));
      });
      return unionQuery;
    }

};


module.exports = sql;



