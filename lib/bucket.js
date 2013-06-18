var util   = require('util')
, url      = require('url')
, extend   = require('extend')
, oops     = require('node-oops')
, dbc      = oops.dbc
, defines  = oops.create
, diff     = require('deep-diff').diff
, apply    = require('deep-diff').applyDiff
, utils    = require('./utils')
, expect   = require('./expect')
, riak     = require('./riak')
, Items    = require('./items').Items
, JsonItem = require('./json_item').JsonItem
, Failure  = require('./failure').Failure;

function Bucket(server, name) {
  dbc([typeof name === 'string', name.length], 'name must be a non-empty string.');
  // server may be either a url or an instance of a server...
  if (server instanceof riak.Riak) {
    server = url.format(server.server);
  }
  Bucket.super_.call(this, server);

  defines(this).enumerable.value('name', name);
  this.__priv.appendPath('buckets/'.concat(encodeURIComponent(name)));
}
util.inherits(Bucket, riak.Riak);

function keys(query, callback) {
  this.get({ path: 'keys', params: { keys: true } },
    this.expect200(callback, this.successOk(callback, 'keys'))
    );
}

function getItems() {
  var items = this.__priv.bucketItems;
  if (!items) {
    items = new Items(url.format(this.server), this.name);
    defines(this.__priv).value('bucketItems', items);
  }
  return items;
}

function getProps(callback) {
  this.get({ path: 'props' }, this.expect200(callback, this.successOk(callback, 'props')));
}

function setProps(props, callback) {
  var that = this;
  this.getProps(function(err, res) {
      if (err) {
        if (callback) callback(err);
      } else {
        var current = res.result;
        var n = {};
        apply(n, current);
        apply(n, props, function(t,s,d) { return d.kind !== 'D'; });
        if (diff(current, n)) {
          that.put({
            path: 'props',
            options: { json: { props: n } }
          },
          that.expect200range(callback, that.successOk(callback)));
        } else {
          if (callback) callback(null, { success: 'OK' });
        }
      }
    });
}

function mapred(spec, callback) {
  try {
    dbc([typeof spec === 'object'], 'spec must be an object.');
    if (typeof spec.inputs === 'undefined' || spec.inputs === null) {
      spec.inputs = this.name;
    }
    if (typeof spec.filters !== 'undefined') {
      spec.filters.bucket = this.name;
    }
    Bucket.super_.prototype.mapred.call(this, spec, callback);
  } catch (err) {
    if (callback) { callback(err); }
  }
}

function createJsonItem(key, data) {
  return new JsonItem(this.items, { key: key }, data);
}

defines(Bucket).enumerable
.property('items', getItems)
;

defines(Bucket).configurable.enumerable
.method(mapred)
.method(keys)
.method(getProps)
.method(setProps)
.method(createJsonItem)
;

module.exports.Bucket = Bucket;
module.exports.create = function(server, name) {
  return new Bucket(server, name);
};
