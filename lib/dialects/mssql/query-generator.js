'use strict';

var Utils = require('../../utils')
  , DataTypes = require('../../data-types')
  , Model = require('../../model')
  , _ = require('lodash')
  , util = require('util')
  , AbstractQueryGenerator = require('../abstract/query-generator');

module.exports = (function() {
  var QueryGenerator = {
    options: {},
    dialect: 'mssql',

    createSchema: function(schema) {
      return [
        'IF NOT EXISTS (SELECT schema_name',
        'FROM information_schema.schemata',
        'WHERE schema_name =', wrapSingleQuote(schema), ')',
        'BEGIN',
          'EXEC sp_executesql N\'CREATE SCHEMA', this.quoteIdentifier(schema),';\'',
        "END;"
      ].join(' ');
    },

    showSchemasQuery: function() {
      return [
        'SELECT "name" as "schema_name" FROM sys.schemas as s',
        'WHERE "s"."name" NOT IN (',
          "'INFORMATION_SCHEMA', 'dbo', 'guest', 'sys', 'archive'",
        ")", "AND", '"s"."name" NOT LIKE', "'db_%'"
      ].join(' ');
    },

    versionQuery: function() {
      return "SELECT @@VERSION as 'version'";
    },

    createTableQuery: function(tableName, attributes, options) {
      var query = "IF OBJECT_ID('<%= table %>', 'U') IS NULL CREATE TABLE <%= table %> (<%= attributes %>)"
        , primaryKeys = []
        , foreignKeys = {}
        , attrStr = []
        , self = this;

      for (var attr in attributes) {
        if (attributes.hasOwnProperty(attr)) {
          var dataType = attributes[attr]
            , match;

          if (Utils._.includes(dataType, 'PRIMARY KEY')) {
            primaryKeys.push(attr);

            if (Utils._.includes(dataType, 'REFERENCES')) {
               // MSSQL doesn't support inline REFERENCES declarations: move to the end
              match = dataType.match(/^(.+) (REFERENCES.*)$/);
              attrStr.push(this.quoteIdentifier(attr) + ' ' + match[1].replace(/PRIMARY KEY/, ''));
              foreignKeys[attr] = match[2];
            } else {
              attrStr.push(this.quoteIdentifier(attr) + ' ' + dataType.replace(/PRIMARY KEY/, ''));
            }
          } else if (Utils._.includes(dataType, 'REFERENCES')) {
            // MSSQL doesn't support inline REFERENCES declarations: move to the end
            match = dataType.match(/^(.+) (REFERENCES.*)$/);
            attrStr.push(this.quoteIdentifier(attr) + ' ' + match[1]);
            foreignKeys[attr] = match[2];
          } else {
            attrStr.push(this.quoteIdentifier(attr) + ' ' + dataType);
          }
        }
      }

      var values = {
        table: this.quoteTable(tableName),
        attributes: attrStr.join(', '),
      }
      , pkString = primaryKeys.map(function(pk) { return this.quoteIdentifier(pk); }.bind(this)).join(', ');

      if (!!options.uniqueKeys) {
        Utils._.each(options.uniqueKeys, function(columns, indexName) {
          if (!Utils._.isString(indexName)) {
            indexName = 'uniq_' + tableName + '_' + columns.fields.join('_');
          }
          values.attributes += ', CONSTRAINT ' + self.quoteIdentifier(indexName) + ' UNIQUE (' + Utils._.map(columns.fields, self.quoteIdentifier).join(', ') + ')';
        });
      }

      if (pkString.length > 0) {
        values.attributes += ', PRIMARY KEY (' + pkString + ')';
      }

      for (var fkey in foreignKeys) {
        if (foreignKeys.hasOwnProperty(fkey)) {
          values.attributes += ', FOREIGN KEY (' + this.quoteIdentifier(fkey) + ') ' + foreignKeys[fkey];
        }
      }

      return Utils._.template(query)(values).trim() + ';';
    },

    describeTableQuery: function(tableName, schema, schemaDelimiter) {
      var sql = [
        "SELECT",
          "c.COLUMN_NAME AS 'Name',",
          "c.DATA_TYPE AS 'Type',",
          "c.IS_NULLABLE as 'IsNull',",
          "COLUMN_DEFAULT AS 'Default'",
        "FROM",
          "INFORMATION_SCHEMA.TABLES t",
        "INNER JOIN",
          "INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME",
        "WHERE t.TABLE_NAME =", wrapSingleQuote(tableName)
      ].join(" ");

      if (schema) {
        sql += "AND t.TABLE_SCHEMA =" + wrapSingleQuote(schema);
      }

      return sql;
    },

    renameTableQuery: function(before, after) {
      var query = 'EXEC sp_rename <%= before %>, <%= after %>;';
      return Utils._.template(query)({
        before: this.quoteTable(before),
        after: this.quoteTable(after)
      });
    },

    showTablesQuery: function () {
      return 'SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES;';
    },

    dropTableQuery: function(tableName, options) {
      var query = "IF OBJECT_ID('<%= table %>', 'U') IS NOT NULL DROP TABLE <%= table %>";
      var values = {
        table: this.quoteTable(tableName)
      };

      return Utils._.template(query)(values).trim() + ";";
    },

    addColumnQuery: function(table, key, dataType) {
      // FIXME: attributeToSQL SHOULD be using attributes in addColumnQuery
      //        but instead we need to pass the key along as the field here
      dataType.field = key;

      var query = 'ALTER TABLE <%= table %> ADD <%= attribute %>;'
        , attribute = Utils._.template('<%= key %> <%= definition %>')({
          key: this.quoteIdentifier(key),
          definition: this.attributeToSQL(dataType, {
            context: 'addColumn'
          })
        });

      return Utils._.template(query)({
        table: this.quoteTable(table),
        attribute: attribute
      });
    },

    removeColumnQuery: function(tableName, attributeName) {
      var query = 'ALTER TABLE <%= tableName %> DROP <%= attributeName %>;';
      return Utils._.template(query)({
        tableName: this.quoteTable(tableName),
        attributeName: this.quoteIdentifier(attributeName)
      });
    },

    changeColumnQuery: function(tableName, attributes) {
      var query = 'ALTER TABLE <%= tableName %> ALTER COLUMN <%= attributes %>;';
      var attrString = [];

      for (var attrName in attributes) {
        var definition = attributes[attrName];

        attrString.push(Utils._.template('<%= attrName %> <%= definition %>')({
          attrName: this.quoteIdentifier(attrName),
          definition: definition
        }));
      }

      return Utils._.template(query)({
        tableName: this.quoteTable(tableName),
        attributes: attrString.join(', ')
      });
    },

    renameColumnQuery: function(tableName, attrBefore, attributes) {
      var query = "EXEC sp_rename '<%= tableName %>.<%= before %>', '<%= after %>', 'COLUMN';"
        , newName = Object.keys(attributes)[0];

      return Utils._.template(query)({
        tableName: this.quoteTable(tableName),
        before: attrBefore,
        after: newName
      });
    },

    bulkInsertQuery: function(tableName, attrValueHashes, options, attributes) {
      var query = 'INSERT INTO <%= table %> (<%= attributes %>)<%= output %> VALUES <%= tuples %>;'
        , emptyQuery = 'INSERT INTO <%= table %><%= output %> DEFAULT VALUES'
        , tuples = []
        , allAttributes = []
        , needIdentityInsertWrapper = false
        , allQueries = []
        , outputFragment;

      if (options.returning) {
        outputFragment = ' OUTPUT INSERTED.*';
      }

      Utils._.forEach(attrValueHashes, function(attrValueHash, i) {
        // special case for empty objects with primary keys
        var fields = Object.keys(attrValueHash);
        if (fields.length === 1 && attributes[fields[0]].autoIncrement && attrValueHash[fields[0]] === null) {
          allQueries.push(emptyQuery);
          return;
        }

        // normal case
        Utils._.forOwn(attrValueHash, function(value, key, hash) {
          if (value !== null && attributes[key].autoIncrement) {
            needIdentityInsertWrapper = true;
          }

          if (allAttributes.indexOf(key) === -1) {
            if (value === null && attributes[key].autoIncrement)
              return;

            allAttributes.push(key);
          }
        });
      });

      if (allAttributes.length > 0) {
        Utils._.forEach(attrValueHashes, function(attrValueHash, i) {
          tuples.push('(' +
            allAttributes.map(function(key) {
              return this.escape(attrValueHash[key]);
            }.bind(this)).join(',') +
          ')');
        }.bind(this));

        allQueries.push(query);
      }

      var replacements = {
        table: this.quoteTable(tableName),
        attributes: allAttributes.map(function(attr) {
                      return this.quoteIdentifier(attr);
                    }.bind(this)).join(','),
        tuples: tuples,
        output: outputFragment
      };

      var generatedQuery = Utils._.template(allQueries.join(';'))(replacements);
      if (needIdentityInsertWrapper) {
        generatedQuery = [
          'SET IDENTITY_INSERT', this.quoteTable(tableName), 'ON;',
          generatedQuery,
          'SET IDENTITY_INSERT', this.quoteTable(tableName), 'OFF;',
        ].join(' ');
      }

      return generatedQuery;
    },

    deleteQuery: function(tableName, where, options) {
      options = options || {};

      var table = this.quoteTable(tableName);
      if (options.truncate === true) {
        // Truncate does not allow LIMIT and WHERE
        return 'TRUNCATE TABLE ' + table;
      }

      where = this.getWhereConditions(where);
      var limit = ''
        , query = 'DELETE<%= limit %> FROM <%= table %><%= where %>; ' +
                  'SELECT @@ROWCOUNT AS AFFECTEDROWS;';

      if (Utils._.isUndefined(options.limit)) {
        options.limit = 1;
      }

      if (!!options.limit) {
        limit = ' TOP(' + this.escape(options.limit) + ')';
      }

      var replacements = {
        limit: limit,
        table: table,
        where: where,
      };

      if (replacements.where) {
        replacements.where = ' WHERE ' + replacements.where;
      }

      return Utils._.template(query)(replacements);
    },

    showIndexesQuery: function(tableName, options) {
      var sql = "EXEC sys.sp_helpindex @objname = N'<%= tableName %>';";
      return Utils._.template(sql)({
        tableName: this.quoteTable(tableName)
      });
    },

    removeIndexQuery: function(tableName, indexNameOrAttributes) {
      var sql = 'DROP INDEX <%= indexName %> ON <%= tableName %>'
        , indexName = indexNameOrAttributes;

      if (typeof indexName !== 'string') {
        indexName = Utils.inflection.underscore(tableName + '_' + indexNameOrAttributes.join('_'));
      }

      var values = {
        tableName: this.quoteIdentifiers(tableName),
        indexName: indexName
      };

      return Utils._.template(sql)(values);
    },

    attributeToSQL: function(attribute, options) {
      if (!Utils._.isPlainObject(attribute)) {
        attribute = {
          type: attribute
        };
      }

      // handle self referential constraints
      if (attribute.Model && attribute.Model.tableName === attribute.references) {
        this.sequelize.log('MSSQL does not support self referencial constraints, '
          + 'we will remove it but we recommend restructuring your query');
        attribute.onDelete = '';
        attribute.onUpdate = '';
      }

      var template;

      if (attribute.type instanceof DataTypes.ENUM) {
        if (attribute.type.values && !attribute.values) attribute.values = attribute.type.values;
        
        // enums are a special case
        template = 'VARCHAR(10) NULL' /* + (attribute.allowNull ? 'NULL' : 'NOT NULL') */;
        template += ' CHECK (' + attribute.field + ' IN(' + Utils._.map(attribute.values, function(value) {
          return this.escape(value);
        }.bind(this)).join(', ') + '))';
        return template;
      } else {
        template = attribute.type.toString();
      }

      if (attribute.allowNull === false) {
        template += ' NOT NULL';
      } else if (!attribute.primaryKey && !Utils.defaultValueSchemable(attribute.defaultValue)) {
        template += ' NULL';
      }

      if (attribute.autoIncrement) {
        template += ' IDENTITY(1,1)';
      }

      // Blobs/texts cannot have a defaultValue
      if (attribute.type !== 'TEXT' && attribute.type._binary !== true &&
          Utils.defaultValueSchemable(attribute.defaultValue)) {
        template += ' DEFAULT ' + this.escape(attribute.defaultValue);
      }

      if (attribute.unique === true) {
        template += ' UNIQUE';
      }

      if (attribute.primaryKey) {
        template += ' PRIMARY KEY';
      }

      if (attribute.references) {
        template += ' REFERENCES ' + this.quoteTable(attribute.references);

        if (attribute.referencesKey) {
          template += ' (' + this.quoteIdentifier(attribute.referencesKey) + ')';
        } else {
          template += ' (' + this.quoteIdentifier('id') + ')';
        }

        if (attribute.onDelete) {
          template += ' ON DELETE ' + attribute.onDelete.toUpperCase();
        }

        if (attribute.onUpdate) {
          template += ' ON UPDATE ' + attribute.onUpdate.toUpperCase();
        }
      }

      return template;
    },

    attributesToSQL: function(attributes, options) {
      var result = {}
        , key
        , attribute
        , existingConstraints = [];

      for (key in attributes) {
        attribute = attributes[key];

        if (attribute.references) {
          if (existingConstraints.indexOf(attribute.references.toString()) !== -1) {
            // no cascading constraints to a table more than once
            attribute.onDelete = '';
            attribute.onUpdate = '';
          } else {
            existingConstraints.push(attribute.references.toString());

            // NOTE: this really just disables cascading updates for all
            //       definitions. Can be made more robust to support the
            //       few cases where MSSQL actually supports them
            attribute.onUpdate = '';
          }

        }

        if (key && !attribute.field) attribute.field = key;
        result[attribute.field || key] = this.attributeToSQL(attribute, options);
      }

      return result;
    },

    findAutoIncrementField: function(factory) {
      var fields = [];
      for (var name in factory.attributes) {
        if (factory.attributes.hasOwnProperty(name)) {
          var definition = factory.attributes[name];

          if (definition && definition.autoIncrement) {
            fields.push(name);
          }
        }
      }

      return fields;
    },

    createTrigger: function(tableName, triggerName, timingType, fireOnArray, functionName, functionParams,
        optionsArray) {
      throwMethodUndefined('createTrigger');
    },

    dropTrigger: function(tableName, triggerName) {
      throwMethodUndefined('dropTrigger');
    },

    renameTrigger: function(tableName, oldTriggerName, newTriggerName) {
      throwMethodUndefined('renameTrigger');
    },

    createFunction: function(functionName, params, returnType, language, body, options) {
      throwMethodUndefined('createFunction');
    },

    dropFunction: function(functionName, params) {
      throwMethodUndefined('dropFunction');
    },

    renameFunction: function(oldFunctionName, params, newFunctionName) {
      throwMethodUndefined('renameFunction');
    },

    quoteIdentifier: function(identifier, force) {
        if (identifier === '*') return identifier;
        return '[' + identifier.replace(/[\[\]']+/g,'') + ']';
    },

    getForeignKeysQuery: function(table, databaseName) {
      var tableName = table.tableName || table;
      var sql = [
        'SELECT',
          'constraint_name = C.CONSTRAINT_NAME',
        'FROM',
          'INFORMATION_SCHEMA.TABLE_CONSTRAINTS C',
        "WHERE C.CONSTRAINT_TYPE = 'FOREIGN KEY'",
        "AND C.TABLE_NAME =", wrapSingleQuote(tableName)
      ].join(' ');

      if (table.schema) {
        sql += " AND C.TABLE_SCHEMA =" + wrapSingleQuote(table.schema);
      }

      return sql;
    },

    dropForeignKeyQuery: function(tableName, foreignKey) {
      return Utils._.template('ALTER TABLE <%= table %> DROP <%= key %>')({
        table: this.quoteTable(tableName),
        key: this.quoteIdentifier(foreignKey)
      });
    },

    setAutocommitQuery: function(value) {
      return '';
      // return 'SET IMPLICIT_TRANSACTIONS ' + (!!value ? 'OFF' : 'ON') + ';';
    },

    setIsolationLevelQuery: function(value, options) {
      if (options.parent) {
        return;
      }

      return 'SET TRANSACTION ISOLATION LEVEL ' + value + ';';
    },

    startTransactionQuery: function(transaction, options) {
      if (options.parent) {
        return 'SAVE TRANSACTION ' + this.quoteIdentifier(transaction.name) + ';';
      }

      return 'BEGIN TRANSACTION;';
    },

    commitTransactionQuery: function(options) {
      if (options.parent) {
        return;
      }

      return 'COMMIT TRANSACTION;';
    },

    rollbackTransactionQuery: function(transaction, options) {
      if (options.parent) {
        return 'ROLLBACK TRANSACTION ' + this.quoteIdentifier(transaction.name) + ';';
      }

      return 'ROLLBACK TRANSACTION;';
    },

    addLimitAndOffset: function(options, model) {
      var fragment = '';
      var offset = options.offset || 0
        , isSubQuery = options.hasIncludeWhere || options.hasIncludeRequired || options.hasMultiAssociation;

      // FIXME: This is ripped from selectQuery to determine whether there is already
      //        an ORDER BY added for a subquery. Should be refactored so we dont' need
      //        the duplication. Also consider moving this logic inside the options.order
      //        check, so that we aren't compiling this twice for every invocation.
      var mainQueryOrder = [];
      var subQueryOrder = [];
      if (options.order) {
        if (Array.isArray(options.order)) {
          options.order.forEach(function(t) {
            if (!Array.isArray(t)) {
              if (isSubQuery && !(t instanceof Model) && !(t.model instanceof Model)) {
                subQueryOrder.push(this.quote(t, model));
              }
            } else {
              if (isSubQuery && !(t[0] instanceof Model) && !(t[0].model instanceof Model)) {
                subQueryOrder.push(this.quote(t, model));
              }
              mainQueryOrder.push(this.quote(t, model));
            }
          }.bind(this));
        } else {
          mainQueryOrder.push(options.order);
        }
      }

      if (options.limit || options.offset) {
        if (!options.order || (!options.order && options.include && !subQueryOrder.length)) {
          fragment += ' ORDER BY ' + this.quoteIdentifier(model.primaryKeyAttribute);
        }

        if (options.offset || options.limit) {
          fragment += ' OFFSET ' + offset + ' ROWS';
        }

        if (options.limit) {
          fragment += ' FETCH NEXT ' + options.limit + ' ROWS ONLY';
        }
      }

      return fragment;
    },

    /*
      Returns a query for selecting elements in the table <tableName>.
      Options:
        - attributes -> An array of attributes (e.g. ['name', 'birthday']). Default: *
        - where -> A hash with conditions (e.g. {name: 'foo'})
                   OR an ID as integer
                   OR a string with conditions (e.g. 'name="foo"').
                   If you use a string, you have to escape it on your own.
        - order -> e.g. 'id DESC'
        - group
        - limit -> The maximum count you want to get.
        - offset -> An offset value to start from. Only useable with limit!
    */

    selectQuery: function(tableName, options, model) {
      // Enter and change at your own peril -- Mick Hansen

      options = options || {};

      var table = null
        , self = this
        , query
        , limit = options.limit
        , mainModel = model
        , mainQueryItems = []
        , mainAttributes = options.attributes && options.attributes.slice(0)
        , mainJoinQueries = []
        // We'll use a subquery if we have a hasMany association and a limit
        , subQuery = options.subQuery === undefined ?
                     limit && options.hasMultiAssociation :
                     options.subQuery
        , subQueryItems = []
        , subQueryAttributes = null
        , subJoinQueries = []
        , mainTableAs = null;

      if (options.tableAs) {
        mainTableAs = this.quoteTable(options.tableAs);
      } else if (!Array.isArray(tableName) && model) {
        options.tableAs = mainTableAs = this.quoteTable(model.name);
      }

      options.table = table = !Array.isArray(tableName) ? this.quoteTable(tableName) : tableName.map(function(t) {
        if (Array.isArray(t)) {
          return this.quoteTable(t[0], t[1]);
        }
        return this.quoteTable(t, true);
      }.bind(this)).join(', ');

      if (subQuery && mainAttributes) {
        model.primaryKeyAttributes.forEach(function(keyAtt) {
          // Check if mainAttributes contain the primary key of the model either as a field or an aliased field
          if (!_.find(mainAttributes, function (attr) {
            return keyAtt === attr || keyAtt === attr[0] || keyAtt === attr[1];
          })) {
            mainAttributes.push(model.rawAttributes[keyAtt].field ? [keyAtt, model.rawAttributes[keyAtt].field] : keyAtt);
          }
        });
      }

      // Escape attributes
      mainAttributes = mainAttributes && mainAttributes.map(function(attr) {
        var addTable = true;

        if (attr._isSequelizeMethod) {
          return self.handleSequelizeMethod(attr);
        }

        if (Array.isArray(attr) && attr.length === 2) {
          if (attr[0]._isSequelizeMethod) {
            attr[0] = self.handleSequelizeMethod(attr[0]);
            addTable = false;
          } else {
            if (attr[0].indexOf('(') === -1 && attr[0].indexOf(')') === -1) {
              attr[0] = self.quoteIdentifier(attr[0]);
            }
          }
          attr = [attr[0], self.quoteIdentifier(attr[1])].join(' AS ');
        } else {
          attr = attr.indexOf(Utils.TICK_CHAR) < 0 && attr.indexOf('"') < 0 ? self.quoteIdentifiers(attr) : attr;
        }

        if (options.include && attr.indexOf('.') === -1 && addTable) {
          attr = mainTableAs + '.' + attr;
        }
        return attr;
      });

      // If no attributes specified, use *
      mainAttributes = mainAttributes || (options.include ? [mainTableAs + '.*'] : ['*']);

      // If subquery, we ad the mainAttributes to the subQuery and set the mainAttributes to select * from subquery
      if (subQuery) {
        // We need primary keys
        subQueryAttributes = mainAttributes;
        mainAttributes = [mainTableAs + '.*'];
      }

      if (options.include) {
        var generateJoinQueries = function(include, parentTable) {
          var table = include.model.getTableName()
            , as = include.as
            , joinQueryItem = ''
            , joinQueries = {
              mainQuery: [],
              subQuery: []
            }
            , attributes
            , association = include.association
            , through = include.through
            , joinType = include.required ? ' INNER JOIN ' : ' LEFT OUTER JOIN '
            , includeWhere = {}
            , whereOptions = Utils._.clone(options)
            , targetWhere;

          whereOptions.keysEscaped = true;

          if (tableName !== parentTable && mainTableAs !== parentTable) {
            as = parentTable + '.' + include.as;
          }

          // includeIgnoreAttributes is used by aggregate functions
          if (options.includeIgnoreAttributes !== false) {

            attributes = include.attributes.map(function(attr) {
              var attrAs = attr,
                  verbatim = false;

              if (Array.isArray(attr) && attr.length === 2) {
                if (attr[0]._isSequelizeMethod) {
                  if (attr[0] instanceof Utils.literal ||
                    attr[0] instanceof Utils.cast ||
                    attr[0] instanceof Utils.fn
                  ) {
                    verbatim = true;
                  }
                }

                attr = attr.map(function($attr) {
                  return $attr._isSequelizeMethod ? self.handleSequelizeMethod($attr) : $attr;
                });

                attrAs = attr[1];
                attr = attr[0];
              } else if (attr instanceof Utils.literal) {
                return attr.val; // We trust the user to rename the field correctly
              } else if (attr instanceof Utils.cast ||
                attr instanceof Utils.fn
              ) {
                throw new Error("Tried to select attributes using Sequelize.cast or Sequelize.fn without specifying an alias for the result, during eager loading. " +
                  "This means the attribute will not be added to the returned instance");
              }

              var prefix;
              if (verbatim === true) {
                prefix = attr;
              } else {
                prefix = self.quoteIdentifier(as) + '.' + self.quoteIdentifier(attr);
              }
              return prefix + ' AS ' + self.quoteIdentifier(as + '.' + attrAs);
            });
            if (include.subQuery && subQuery) {
              subQueryAttributes = subQueryAttributes.concat(attributes);
            } else {
              mainAttributes = mainAttributes.concat(attributes);
            }
          }

          if (through) {
            var throughTable = through.model.getTableName()
              , throughAs = as + '.' + through.as
              , throughAttributes = through.attributes.map(function(attr) {
                return self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(Array.isArray(attr) ? attr[0] : attr) +
                       ' AS ' +
                       self.quoteIdentifier(throughAs + '.' + (Array.isArray(attr) ? attr[1] : attr));
              })
              , primaryKeysSource = association.source.primaryKeyAttributes
              , tableSource = parentTable
              , identSource = association.identifierField
              , attrSource = primaryKeysSource[0]
              , where

              , primaryKeysTarget = association.target.primaryKeyAttributes
              , tableTarget = as
              , identTarget = association.foreignIdentifierField
              , attrTarget = association.target.rawAttributes[primaryKeysTarget[0]].field || primaryKeysTarget[0]

              , sourceJoinOn
              , targetJoinOn

              , throughWhere;

            if (options.includeIgnoreAttributes !== false) {
              // Through includes are always hasMany, so we need to add the attributes to the mainAttributes no matter what (Real join will never be executed in subquery)
              mainAttributes = mainAttributes.concat(throughAttributes);
            }

            // Figure out if we need to use field or attribute
            if (!subQuery) {
              attrSource = association.source.rawAttributes[primaryKeysSource[0]].field;
            }
            if (subQuery && !include.subQuery && !include.parent.subQuery && include.parent.model !== mainModel) {
              attrSource = association.source.rawAttributes[primaryKeysSource[0]].field;
            }

            // Filter statement for left side of through
            // Used by both join and subquery where

            // If parent include was in a subquery need to join on the aliased attribute
            if (subQuery && !include.subQuery && include.parent.subQuery) {
              sourceJoinOn = self.quoteIdentifier(tableSource + '.' + attrSource) + ' = ';
            } else {
              sourceJoinOn = self.quoteTable(tableSource) + '.' + self.quoteIdentifier(attrSource) + ' = ';
            }
            sourceJoinOn += self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(identSource);

            // Filter statement for right side of through
            // Used by both join and subquery where
            targetJoinOn = self.quoteIdentifier(tableTarget) + '.' + self.quoteIdentifier(attrTarget) + ' = ';
            targetJoinOn += self.quoteIdentifier(throughAs) + '.' + self.quoteIdentifier(identTarget);

            if (include.through.where) {
              throughWhere = self.getWhereConditions(include.through.where, self.sequelize.literal(self.quoteIdentifier(throughAs)), include.through.model);
            }

            if (self._dialect.supports.joinTableDependent) {
              // Generate a wrapped join so that the through table join can be dependent on the target join
              joinQueryItem += joinType + '(';
              joinQueryItem += self.quoteTable(throughTable, throughAs);
              joinQueryItem += ' INNER JOIN ' + self.quoteTable(table, as) + ' ON ';
              joinQueryItem += targetJoinOn;

              if (throughWhere) {
                joinQueryItem += ' AND ' + throughWhere;
              }

              joinQueryItem += ') ON '+sourceJoinOn;
            } else {
              // Generate join SQL for left side of through
              joinQueryItem += joinType + self.quoteTable(throughTable, throughAs)  + ' ON ';
              joinQueryItem += sourceJoinOn;

              // Generate join SQL for right side of through
              joinQueryItem += joinType + self.quoteTable(table, as) + ' ON ';
              joinQueryItem += targetJoinOn;

              if (throughWhere) {
                joinQueryItem += ' AND ' + throughWhere;
              }

            }

            if (include.where || include.through.where) {
              if (include.where) {
                targetWhere = self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions);
                if (targetWhere) {
                  joinQueryItem += ' AND ' + targetWhere;
                }
              }
              if (subQuery && include.required) {
                if (!options.where) options.where = {};
                (function (include) {
                  // Closure to use sane local variables

                  var parent = include
                    , child = include
                    , nestedIncludes = []
                    , topParent
                    , topInclude
                    , $query;

                  while (parent = parent.parent) {
                    nestedIncludes = [_.extend({}, child, {include: nestedIncludes})];
                    child = parent;
                  }

                  topInclude = nestedIncludes[0];
                  topParent = topInclude.parent;

                  if (topInclude.through && Object(topInclude.through.model) === topInclude.through.model) {
                    $query = self.selectQuery(topInclude.through.model.getTableName(), {
                      attributes: [topInclude.through.model.primaryKeyAttributes[0]],
                      include: [{
                        model: topInclude.model,
                        as: topInclude.model.name,
                        attributes: [],
                        association: {
                          associationType: 'BelongsTo',
                          isSingleAssociation: true,
                          source: topInclude.association.target,
                          target: topInclude.association.source,
                          identifier: topInclude.association.foreignIdentifier,
                          identifierField: topInclude.association.foreignIdentifierField
                        },
                        required: true,
                        include: topInclude.include,
                        _pseudo: true
                      }],
                      where: self.sequelize.and(
                        self.sequelize.asIs([
                          self.quoteTable(topParent.model.name) + '.' + self.quoteIdentifier(topParent.model.primaryKeyAttributes[0]),
                          self.quoteIdentifier(topInclude.through.model.name) + '.' + self.quoteIdentifier(topInclude.association.identifierField)
                        ].join(" = ")),
                        topInclude.through.where
                      ),
                      limit: 1,
                      includeIgnoreAttributes: false
                    }, topInclude.through.model);
                  } else {
                    $query = self.selectQuery(topInclude.model.tableName, {
                      attributes: [topInclude.model.primaryKeyAttributes[0]],
                      include: topInclude.include,
                      where: {
                        $join: self.sequelize.asIs([
                          self.quoteTable(topParent.model.name) + '.' + self.quoteIdentifier(topParent.model.primaryKeyAttributes[0]),
                          self.quoteIdentifier(topInclude.model.name) + '.' + self.quoteIdentifier(topInclude.association.identifierField)
                        ].join(" = "))
                      },
                      limit: 1,
                      includeIgnoreAttributes: false
                    }, topInclude.model);
                  }

                  options.where['__' + throughAs] = self.sequelize.asIs([
                    '(',
                      $query.replace(/\;$/, ""),
                    ')',
                    'IS NOT NULL'
                  ].join(' '));
                })(include);
              }
            }
          } else {
            var left = association.source
              , right = association.target
              , primaryKeysLeft = left.primaryKeyAttributes
              , primaryKeysRight = right.primaryKeyAttributes
              , tableLeft = parentTable
              , attrLeft = association.associationType === 'BelongsTo' ?
                           association.identifierField || association.identifier :
                           primaryKeysLeft[0]

              , tableRight = as
              , attrRight = association.associationType !== 'BelongsTo' ?
                            association.identifierField || association.identifier :
                            right.rawAttributes[primaryKeysRight[0]].field || primaryKeysRight[0]
              , joinOn
              , subQueryJoinOn;

            // Filter statement
            // Used by both join and where
            if (subQuery && !include.subQuery && include.parent.subQuery && (include.hasParentRequired || include.hasParentWhere || include.parent.hasIncludeRequired || include.parent.hasIncludeWhere)) {
              joinOn = self.quoteIdentifier(tableLeft + '.' + attrLeft);
            } else {
              if (association.associationType !== 'BelongsTo') {
                // Alias the left attribute if the left attribute is not from a subqueried main table
                // When doing a query like SELECT aliasedKey FROM (SELECT primaryKey FROM primaryTable) only aliasedKey is available to the join, this is not the case when doing a regular select where you can't used the aliased attribute
                if (!subQuery || (subQuery && include.parent.model !== mainModel)) {
                  if (left.rawAttributes[attrLeft].field) {
                    attrLeft = left.rawAttributes[attrLeft].field;
                  }
                }
              }
              joinOn = self.quoteTable(tableLeft) + '.' + self.quoteIdentifier(attrLeft);
            }
            subQueryJoinOn = self.quoteTable(tableLeft) + '.' + self.quoteIdentifier(attrLeft);

            joinOn += ' = ' + self.quoteTable(tableRight) + '.' + self.quoteIdentifier(attrRight);
            subQueryJoinOn += ' = ' + self.quoteTable(tableRight) + '.' + self.quoteIdentifier(attrRight);

            if (include.where) {
              targetWhere = self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions);
              if (targetWhere) {
                joinOn += ' AND ' + targetWhere;
                subQueryJoinOn += ' AND ' + targetWhere;
              }
            }

            // If its a multi association and the main query is a subquery (because of limit) we need to filter based on this association in a subquery
            if (subQuery && association.isMultiAssociation && include.required) {
              if (!options.where) options.where = {};
              // Creating the as-is where for the subQuery, checks that the required association exists
              var $query = self.selectQuery(include.model.getTableName(), {
                tableAs: as,
                attributes: [attrRight],
                where: self.sequelize.asIs(subQueryJoinOn ? [subQueryJoinOn] : [joinOn]),
                limit: 1
              }, include.model);

              var subQueryWhere = self.sequelize.asIs([
                '(',
                  $query.replace(/\;$/, ""),
                ')',
                'IS NOT NULL'
              ].join(' '));

              if (options.where instanceof Utils.and) {
                options.where.args.push(subQueryWhere);
              } else if (Utils._.isPlainObject(options.where)) {
                options.where['__' + as] = subQueryWhere;
              } else {
                options.where = new Utils.and(options.where, subQueryWhere);
              }
            }

            // Generate join SQL
            joinQueryItem += joinType + self.quoteTable(table, as) + ' ON ' + joinOn;
          }

          if (include.subQuery && subQuery) {
            joinQueries.subQuery.push(joinQueryItem);
          } else {
            joinQueries.mainQuery.push(joinQueryItem);
          }

          if (include.include) {
            include.include.forEach(function(childInclude) {
              if (childInclude._pseudo) return;
              var childJoinQueries = generateJoinQueries(childInclude, as);

              if (childInclude.subQuery && subQuery) {
                joinQueries.subQuery = joinQueries.subQuery.concat(childJoinQueries.subQuery);
              }
              if (childJoinQueries.mainQuery) {
                joinQueries.mainQuery = joinQueries.mainQuery.concat(childJoinQueries.mainQuery);
              }

            }.bind(this));
          }

          return joinQueries;
        };

        // Loop through includes and generate subqueries
        options.include.forEach(function(include) {
          var joinQueries = generateJoinQueries(include, options.tableAs);

          subJoinQueries = subJoinQueries.concat(joinQueries.subQuery);
          mainJoinQueries = mainJoinQueries.concat(joinQueries.mainQuery);

        }.bind(this));
      }

      // If using subQuery select defined subQuery attributes and join subJoinQueries
      if (subQuery) {
        subQueryItems.push('SELECT ' + subQueryAttributes.join(', ') + ' FROM ' + options.table);
        if (mainTableAs) {
          subQueryItems.push(' AS ' + mainTableAs);
        }
        subQueryItems.push(subJoinQueries.join(''));

      // Else do it the reguar way
      } else {
        mainQueryItems.push('SELECT ' + (options.distinct ? 'DISTINCT ':'') + mainAttributes.join(', ') + ' FROM ' + options.table);
        if (mainTableAs) {
          mainQueryItems.push(' AS ' + mainTableAs);
        }
        mainQueryItems.push(mainJoinQueries.join(''));
      }

      // Add WHERE to sub or main query
      if (options.hasOwnProperty('where')) {
        options.where = this.getWhereConditions(options.where, mainTableAs || tableName, model, options);
        if (options.where) {
          if (subQuery) {
            subQueryItems.push(' WHERE ' + options.where);
          } else {
            mainQueryItems.push(' WHERE ' + options.where);
          }
        }
      }

      // Add GROUP BY to sub or main query
      if (options.group) {
        options.group = Array.isArray(options.group) ? options.group.map(function(t) { return this.quote(t, model); }.bind(this)).join(', ') : options.group;
        if (subQuery) {
          subQueryItems.push(' GROUP BY ' + options.group);
        } else {
          mainQueryItems.push(' GROUP BY ' + options.group);
        }
      }

      // Add HAVING to sub or main query
      if (options.hasOwnProperty('having')) {
        options.having = this.getWhereConditions(options.having, tableName, model, options, false);
        if (subQuery) {
          subQueryItems.push(' HAVING ' + options.having);
        } else {
          mainQueryItems.push(' HAVING ' + options.having);
        }
      }
      // Add ORDER to sub or main query
      if (options.order) {
        var mainQueryOrder = [];
        var subQueryOrder = [];

        var validateOrder = function(order) {
          if (order instanceof Utils.literal) return;

          if (!_.contains([
            'ASC',
            'DESC',
            'ASC NULLS LAST',
            'DESC NULLS LAST',
            'ASC NULLS FIRST',
            'DESC NULLS FIRST',
            'NULLS FIRST',
            'NULLS LAST'
          ], order.toUpperCase())) {
            throw new Error(util.format('Order must be \'ASC\' or \'DESC\', \'%s\' given', order));
          }
        };

        if (Array.isArray(options.order)) {
          options.order.forEach(function(t) {
            if (Array.isArray(t) && _.size(t) > 1) {
              if (t[0] instanceof Model || t[0].model instanceof Model) {
                if (typeof t[t.length - 2] === "string") {
                  validateOrder(_.last(t));
                }
              } else {
                validateOrder(_.last(t));
              }
            }

            if (subQuery && (Array.isArray(t) && !(t[0] instanceof Model) && !(t[0].model instanceof Model))) {
              subQueryOrder.push(this.quote(t, model));
            }

            mainQueryOrder.push(this.quote(t, model));
          }.bind(this));
        } else {
          mainQueryOrder.push(this.quote(typeof options.order === "string" ? new Utils.literal(options.order) : options.order, model));
        }

        if (mainQueryOrder.length) {
          mainQueryItems.push(' ORDER BY ' + mainQueryOrder.join(', '));
        }
        if (subQueryOrder.length) {
          subQueryItems.push(' ORDER BY ' + subQueryOrder.join(', '));
        }
      }

      // Add LIMIT, OFFSET to sub or main query
      var limitOrder = this.addLimitAndOffset(options, model);
      if (limitOrder) {
        if (subQuery) {
          subQueryItems.push(limitOrder);
        } else {
          mainQueryItems.push(limitOrder);
        }
      }

      // If using subQuery, select attributes from wrapped subQuery and join out join tables
      if (subQuery) {
        query = 'SELECT ' + (options.distinct ? 'DISTINCT ':'') + mainAttributes.join(', ') + ' FROM (';
        query += subQueryItems.join('');
        query += ') AS ' + options.tableAs;
        query += mainJoinQueries.join('');
        query += mainQueryItems.join('');
      } else {
        query = mainQueryItems.join('');
      }

      if (options.lock && this._dialect.supports.lock) {
        if (options.lock === 'SHARE') {
          query += ' ' + this._dialect.supports.forShare;
        } else {
          query += ' FOR UPDATE';
        }
      }

      query += ';';

      return query;
    },

    findAssociation: function(attribute, dao) {
      throwMethodUndefined('findAssociation');
    },

    getAssociationFilterDAO: function(filterStr, dao) {
      throwMethodUndefined('getAssociationFilterDAO');
    },

    getAssociationFilterColumn: function(filterStr, dao, options) {
      throwMethodUndefined('getAssociationFilterColumn');
    },

    getConditionalJoins: function(options, originalDao) {
      throwMethodUndefined('getConditionalJoins');
    },

    booleanValue: function(value) {
      return !!value ? 1 : 0;
    }
  };

  // private methods
  function wrapSingleQuote(identifier){
    return Utils.addTicks(identifier, "'");
  }

  /* istanbul ignore next */
  var throwMethodUndefined = function(methodName) {
    throw new Error('The method "' + methodName + '" is not defined! Please add it to your sql dialect.');
  };

  return Utils._.extend(Utils._.clone(AbstractQueryGenerator), QueryGenerator);
})();
