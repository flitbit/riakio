var util   = require('util')
, url      = require('url')
, extend   = util._extend
, dbc      = require('dbc.js')
, diff     = require('deep-diff').diff
, apply    = require('deep-diff').applyDiff
, utils    = require('./utils')
, Riak     = require('./riak')
, Items    = require('./items')
, JsonItem = require('./json_item')
, Search   = require('./search')
, webflow  = require('webflow')
, Success  = webflow.Success
;

function Bucket(options) {
  dbc(typeof options === 'object', 'options must be an object.');
  dbc(typeof options.bucket === 'string', 'options must contain a bucket name.');
  Bucket.super_.call(this, options);
  Object.defineProperty(this, 'name', { value: options.bucket, enumerable: true });
  this.appendPath('buckets/'.concat(encodeURIComponent(options.bucket)));
  if (options.calculateKey) {
    Object.defineProperty(this, 'calculateKey', { value: options.calculateKey, enumerable: true });
  }
  if (options.index) {
    Object.defineProperty(this, 'index', { value: options.index, enumerable: true });
  }
}
util.inherits(Bucket, Riak);

Object.defineProperties(Bucket.prototype, {

  search: {
    get: function() {
      var search = this._state.search;
      if (!search) {
        search = new Search(this);
        this._state.search = search;
      }
      return search;
    },
    enumerable: true
  },

  keys: {
    value: function(filters, callback) {
      var mr = {
       phases: [{
        map: function(v) {
          return [v.key];
        }}],
        raw_data: true
      };
      if (filters) {
        mr.inputs = { key_filters: filters.encoded };
      }
      this.mapred(mr, callback);
    },
    enumerable: true
  },

  items: {
    get: function() {
      var items = this._state.bucketItems;
      if (!items) {
        items = new Items(this);
        this._state.bucketItems = items;
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
        var inputsType = typeof spec.inputs
        , encodedName = encodeURIComponent(this.name)
        ;
        if ('undefined' === inputsType) {
          spec.inputs = encodedName;
        }
        if (('object' === inputsType && !Array.isArray(spec.inputs))
           && (spec.inputs.hasOwnProperty('query')
            || spec.inputs.hasOwnProperty('index')
            || spec.inputs.hasOwnProperty('key_filters'))) {
            spec.inputs.bucket = encodedName;
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
      } else {
        meta.key = this.calculateKey(data);
      }
      return new JsonItem(this.items, meta, data);
    },
    enumerable: true
  },

  prepareHeaders: {
    value: function(body, meta) {
      var headers = Riak.prototype.prepareHeaders.call(this, body, meta)
      , index = this.index
      ;
      if (index && index.length) {
        index.forEach(function(idx) {
          idx.indexFor(body, headers);
        });
      }
      return headers;
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