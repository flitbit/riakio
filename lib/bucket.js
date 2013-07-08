var util   = require('util')
, url      = require('url')
, extend   = require('extend')
, dbc      = require('dbc.js')
, diff     = require('deep-diff').diff
, apply    = require('deep-diff').applyDiff
, utils    = require('./utils')
, Riak     = require('./riak')
, Items    = require('./items')
, JsonItem = require('./json_item')
, webflow  = require('webflow')
, Success  = webflow.Success
;

function Bucket(options, name) {
  dbc([typeof name === 'string', name.length], 'name must be a non-empty string.');
  Bucket.super_.call(this, options);

  Object.defineProperties(this, {
    name: { value: name, enumerable: true }
  });
  this.__priv.appendPath('buckets/'.concat(encodeURIComponent(name)));
}
util.inherits(Bucket, Riak);

Object.defineProperties(Bucket.prototype, {

 keys: {
  value: function(query, callback) {
    this.mapred({
      phases: [{
        map: function(v) {
          return [v.key];
        }
      }],
      raw_data: true
    },
    this.expect200(callback, this.successOk(callback, function(r){ return r.body; }))
    );
  },
  enumerable: true
},

items: {
  get: function() {
    var items = this.__priv.bucketItems;
    if (!items) {
      items = new Items(this.__options, this.name);
      this.__priv.bucketItems = items;
    }
    return items;
  },
  enumerable: true
},

getProps: {
  value: function(callback) {
    this.get({ path: 'props' }, this.expect200(callback, this.successOk(callback, 'props')));
  },
  enumerable: true
},

setProps: {
  value: function(props, callback) {
    var that = this;
    this.getProps(
        function(err, res) {
          if (err) {
            callback(err);
            return;
          }
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
            if (callback) callback(null, Success.ok());
          }
        });
    },
    enumerable: true
  },

  ensureKvSearch: {
    value: function(callback) {
      var that = this;
      this.getProps(
        function(err, res) {
          if (err) {
            callback(err);
            return;
          }
          var current = res.result
          , hook
          ;
          current.precommit.forEach(function(ea) {
            if (ea.mod && ea.mod === 'riak_search_kv_hook') {
              hook = ea;
            }
          });
          if (hook) {
            callback(null, res);
          } else {
            current.precommit.push({ mod: 'riak_search_kv_hook', fun: 'precommit' });
             that.put({
              path: 'props',
              options: { json: { props: current } }
            },
            that.expect200range(callback, that.successOk(callback)));
          }
        });
    },
    enumerable: true
  },

  mapred: {
    value: function(spec, callback) {
      try {
        dbc([typeof spec === 'object'], 'spec must be an object.');
        if (spec.query) {
          spec.query.bucket = encodeURIComponent(this.name);
        }
        else {
          if (typeof spec.inputs === 'undefined' || spec.inputs === null) {
            spec.inputs = this.name;
          }
          if (typeof spec.filters !== 'undefined') {
            spec.filters.bucket = this.name;
          }
        }
        Bucket.super_.prototype.mapred.call(this, spec, callback);
      } catch (err) {
        if (callback) { callback(err); }
      }
    },
    enumerable: true
  },

  createJsonItem: {
    value: function(data, key) {
      var meta = {};
      if ('undefined' !== typeof key) {
        meta.key = key;
      }
      return new JsonItem(this.items, meta, data);
    },
    enumerable: true
  }

});

Object.defineProperties(Bucket, {

  create: {
    value: function(options, name) {
      return new Bucket(options, name);
    },
    enumerable: true
  }

})

module.exports = Bucket;