var util   = require('util')
, url      = require('url')
, extend   = require('extend')
, oops     = require('node-oops')
, dbc      = oops.dbc
, defines  = oops.create
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
    this.expect200(callback, callback)
    );
}

function peelBucketsFromLinks(err, res, callback) {
  if (err) { callback(err); }
  else {
    var keys = [];
    res.meta.links.forEach(function(link) {
      keys.push(link.key);
    });
    callback(null, keys);
  }
}

function getItems() {
  var items = this.__priv.bucketItems;
  if (!items) {
    items = new Items(url.format(this.server), this.name);
    this.__priv.declares.value('bucketItems', items);
  }
  return items;
}

function getProps(callback) {
  this.get({ path: 'props' }, this.expect200(callback, callback));
}

function setProps(props, callback) {
  var that = this;
  this.getProps({ path: 'props' },
    this.expect200(callback, function(err, res) {
      var current = res.body.props;
      var n = {};
      apply(n, current);
      apply(n, props, function(t,s,d) { return d.kind !== 'D'; });
      if (diff(current, n)) {
        that.put({
          path: 'props',
          options: { json: { props: n } }
        },
        that.expect200range(callback, callback));
      }
    }));
}

function mapred(spec, callback) {
  try {
    dbc([typeof spec === 'object'], 'spec must be an object.');
    if (typeof spec.inputs === 'undefined' || spec.inputs === null) {
      spec.inputs = this.name;
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
