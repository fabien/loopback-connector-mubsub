var _ = require('lodash');
var fs = require('fs');
var os = require('os');
var path = require('path');
var util = require('util');
var async = require('async');
var assert = require('assert');
var mubsub = require('mubsub');

var Connector = require('loopback-connector').Connector;
var mongodb = require('mongodb');

var debug = require('debug')('loopback:connector:mubsub');

/**
 * Export the Mubsub class.
 */

module.exports = Mubsub;

/**
 * Create an instance of the connector with the given `settings`.
 */

function Mubsub(settings, dataSource) {
    Connector.call(this, 'mubsub', settings);
    
    assert(typeof settings === 'object', 'cannot initialize Mubsub without a settings object');
    assert(typeof settings.url === 'string', 'cannot initialize Mubsub without a MongoDB url');
    
    this.debug = settings.debug || debug.enabled;
    this.settings = _.omit(settings, 'defaults');
    this.defaults = _.extend({}, settings.defaults);
    this.channels = {};
};

util.inherits(Mubsub, Connector);

Mubsub.version = '1.0.0';

Mubsub.initialize = function(dataSource, callback) {
    var connector = new Mubsub(dataSource.settings);
    dataSource.connector = connector; // Attach connector to dataSource
    connector.dataSource = dataSource; // Hold a reference to dataSource
    connector.connect(callback);
};

Mubsub.prototype.define = function(modelDefinition) {
    Connector.prototype.define.call(this, modelDefinition);
    var self = this;
    var model = modelDefinition.model;
    model.defineProperty('event', { type: 'string', required: true });
    model.publish = function(eventName, message, callback) {
        if (_.isFunction(message)) callback = message, message = {};
        var data = _.extend({ event: eventName }, message);
        this.create(data, callback);
    };
};

Mubsub.prototype.connect = function (callback) {
    this.client = mubsub(this.settings.url);
    this.client.once('error', function(err) { callback(err); });
    this.client.once('connect', function() { callback(); });
};

Mubsub.prototype.disconnect = function() {
    if (this.client) this.client.close();
};

Mubsub.prototype.automigrate = function(models, callback) {
    var self = this;
    if (this.client) {
        async.each(models, function(model, next) {
            var collection = self.collectionName(model);
            self.client.db.dropCollection(collection, function (err, collection) {
                if (err && !(err.name === 'MongoError'
                    && err.ok === 0 && err.errmsg === 'ns not found')) {
                    // For errors other than 'ns not found' (collection doesn't exist)
                    return next(err);
                }
                process.nextTick(next);
            });
        }, callback);
    } else {
        self.dataSource.once('connected', function() {
            self.automigrate(models, callback);
        });
    }
};

Mubsub.prototype.ensureChannel = function(channelName) {
    if (this.channels[channelName]) return this.channels[channelName];
    var channel = this.client.channel(this.collectionName(channelName));
    var model = this.dataSource.models[channelName];
    if (model) {
        var prefix = this.settings.prefix || '';
        model.channel = channel;
        channel.on('document', function(doc) {
            model.emit(prefix + doc.event, doc.message || {});
            model.emit('message', doc);
        });
    }
    this.channels[channelName] = channel;
    return channel;
};

Mubsub.prototype.collectionName = function(channelName) {
    var model = this.dataSource.models[channelName];
    if (model && model.settings && _.isObject(model.settings.mubsub)) {
        return model.settings.mubsub.collection || channelName;
    }
    return channelName;
};

Mubsub.prototype.collection = function(channelName) {
    if (this.channels[channelName]) {
        var collectionName = this.collectionName(channelName);
        return this.client.db.collection(collectionName);
    }
};

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Mubsub.prototype.create = function (model, data, callback) {
    if (!_.isObject(data) || _.isEmpty(data) || _.isEmpty(data.event)) {
        callback(new Error('Attribute `event` is required.'));
    } else {
        var channel = this.ensureChannel(model);
        channel.publish(data.event, _.omit(data, 'event'), function(err, entry) {
            callback(err, entry && entry._id);
        });
    }
};
 
/**
 * Check if a model instance exists by id
 */
Mubsub.prototype.exists = function (model, id, callback) {
    this.find(model, id, function(err, entry) {
        callback(err, entry ? true : false);
    });
};

/**
 * Find a model instance by id
 * @param {Function} callback - you must provide an array of results to the callback as an argument
 */
Mubsub.prototype.find = function find(model, id, callback) {
    var collection = this.collection(model);
    if (collection) {
        var self = this;
        collection.findOne({ _id: ObjectID(id) }, function(err, data) {
            callback && callback(err, data ? self.fromDB(data) : null);
        });
    } else {
        callback(new Error('Invalid channel: `' + model + '`.'));
    }
};

/**
 * Query model instances by the filter
 */
Mubsub.prototype.all = function all(model, filter, callback) {
    var collection = this.collection(model);
    if (collection) {
        var self = this;
        var where = this.whereQuery(model, filter.where || {});
        var cursor = this.collection(model).find(where);
        // filter.sort (raw MongoDB), NOT filter.order
        cursor.sort(filter.sort || { $natural: -1 });
        if (filter.limit) cursor.limit(filter.limit);
        if (filter.skip) {
            cursor.skip(filter.skip);
        } else if (filter.offset) {
            cursor.skip(filter.offset);
        }
        cursor.toArray(function (err, data) {
            if (err) return callback(err);
            var entries = _.map(data, self.fromDB);
            callback(null, entries);
        });
    } else {
        callback(new Error('Invalid channel: `' + model + '`.'));
    }
};

/**
 * Delete all model instances
 */
Mubsub.prototype.destroyAll = function destroyAll(model, where, callback) {
    if (_.isFunction(where)) callback = where, where = {};
    var collection = this.collection(model);
    if (collection) {
        var where = this.whereQuery(model, where || {});
        collection.remove(where, function(err, results) {
            if (err) return callback(err);
            var count = typeof results.result.n === 'number' ? results.result.n : 0;
            callback && callback(null, {count: count});
        });
    } else {
        callback(new Error('Invalid channel: `' + model + '`.'));
    }
};

/**
 * Count the model instances by the where criteria
 */
Mubsub.prototype.count = function count(model, callback, where) {
    var collection = this.collection(model);
    if (collection) {
        where = this.whereQuery(model, where || {});
        collection.count(where, function(err, count) {
            callback && callback(err, count);
        });
    } else {
        callback(new Error('Invalid channel: `' + model + '`.'));
    }
};

/**
 * Update the attributes for a model instance by id
 */
Mubsub.prototype.updateAttributes = function updateAttributes(model, id, data, callback) {
    callback(new Error(model + ' is immutable.'));
};

/**
 * Save a model instance
 */
Mubsub.prototype.save = function (model, data, callback) {
    callback(new Error(model + ' is immutable.'));
};

/**
 * Delete a model instance by id
 */
Mubsub.prototype.destroy = function destroy(model, id, callback) {
    callback(new Error(model + ' is immutable.'));
};

Mubsub.prototype.fromDB = function(doc) {
    var entry = {};
    entry.id = doc._id;
    _.extend(entry, _.omit(doc, '_id', 'message'));
    _.extend(entry, doc.message);
    return entry;
};

Mubsub.prototype.whereQuery = function(model, where) {
    where = this.buildWhere(model, where);
    var skipDummy = { dummy: { $ne: true } };
    if (_.isEmpty(where)) {
        where = skipDummy;
    } else {
        where = { $and: [skipDummy, where] };
    }
    return where;
};

Mubsub.prototype.buildWhere = function(model, where) {
    var self = this;
    var query = {};
    if (where === null || (typeof where !== 'object')) {
        return query;
    }
    var idName = self.idName(model);
    Object.keys(where).forEach(function (k) {
        var cond = where[k];
        if (k === idName) {
            k = '_id';
            cond = ObjectID(cond);
        }
        if (k === 'and' || k === 'or' || k === 'nor') {
            if (Array.isArray(cond)) {
                cond = cond.map(function (c) {
                    return self.buildWhere(model, c);
                });
            }
            query['$' + k ] = cond;
            delete query[k];
            return;
        }
        var spec = false;
        var options = null;
        if (cond && cond.constructor.name === 'Object') {
            options = cond.options;
            spec = Object.keys(cond)[0];
            cond = cond[spec];
        }
        if (spec) {
            if (spec === 'between') {
                query[k] = { $gte: cond[0], $lte: cond[1]};
            } else if (spec === 'inq') {
                query[k] = { $in: cond.map(function (x) {
                    if ('string' !== typeof x) return x;
                    return ObjectID(x);
                })};
            } else if (spec === 'like') {
                query[k] = {$regex: new RegExp(cond, options)};
            } else if (spec === 'nlike') {
                query[k] = {$not: new RegExp(cond, options)};
            } else if (spec === 'neq') {
                query[k] = {$ne: cond};
            } else {
                query[k] = {};
                query[k]['$' + spec] = cond;
            }
        } else {
            if (cond === null) {
                // http://docs.mongodb.org/manual/reference/operator/query/type/
                // Null: 10
                query[k] = {$type: 10};
            } else {
                query[k] = cond;
            }
        }
    });
    return query;
}

/*!
 * Convert the id to be a BSON ObjectID if it is compatible
 * @param {*} id The id value
 * @returns {ObjectID}
 */
function ObjectID(id) {
    if (id instanceof mongodb.ObjectID) return id;
    if (typeof id !== 'string') return id;
    try {
        // MongoDB's ObjectID constructor accepts number, 12-byte string or 24-byte
        // hex string. For LoopBack, we only allow 24-byte hex string, but 12-byte
        // string such as 'line-by-line' should be kept as string
        if (/^[0-9a-fA-F]{24}$/.test(id)) {
          return new mongodb.ObjectID(id);
        } else {
          return id;
        }
    } catch (e) {
        return id;
    }
}
