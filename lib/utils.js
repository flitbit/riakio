/*jshint indent:2, laxcomma:true, node:true*/
/*global require:false*/
'use strict';

var util    = require('util')
  , oops    = require('node-oops')
  , defines = oops.create
  , extend  = require('extend')
  , qs      = require('querystring')
	, _       = require('lodash')
	;

var makeLinks,
	filtered_headers = ['server', 'vary'],
	__sequence = 0;

function sequence() {
	return padL(__sequence++, 10);
}

function filterHeaders(head) {
	var o = {};
	Object.keys(head).forEach(function (k) {
		if (filtered_headers.indexOf(k) < 0) {
			switch (k) {
				case 'content-length':
					o[k] = parseInt(head[k], 10);
					break;
				case 'last-modified':
					o[k] = head[k];
					o['last-modified-sort'] = new Date(head[k]).toISOString();
					break;
				case 'link':
					o.links = makeLinks(head[k]);
					break;
				default:
					o[k] = head[k];
					break;
			}
		}
	});
	return o;
}

function padL(n, len) {
	// JSLINT doesn't like the next line but we're purposely initializing the len.
	return (new Array(len - String(n).length + 1)).join("0").concat(n);
}

function extractBoundary(header) {
	var c = header.match(/boundary=([A-Za-z0-9\'()+_,-.\/:=?]+)/);
	if (c) {
		return c[1];
	}
}

function parseHttpHeadersAndBody(part) {
	var body, headers, parsed, res, md = part.split(/\r?\n\r?\n/);
	if (md){
		headers = md[0];
		body = md[1];
		parsed = {};
		headers.split(/\r?\n/).forEach(function(header) {
			var ea = header.split(': '),
			k = ea[0],
			v = ea[1];
			return (parsed[k.toLowerCase()] = v);
		});
		headers = filterHeaders(parsed);
		return {
			headers: headers,
			body: body
		};
	}
}

function decodeMultipartPart(part) {
	if (typeof part !== 'object') {
		throw new TypeError('[Object] part must be an object');
	}
	if (part.body) {
		var contentType = ((part.headers) ? part.headers['content-type'] : undefined) || '';
		if (contentType.match(/^application\/binary/)) {
			part.body = new Buffer(part.body, 'binary');
		} else if (contentType.match(/^application\/octet-stream/)) {
			part.body = new Buffer(part.body, 'binary');
		} else if (contentType.match(/^application\/json/)) {
			part.body = JSON.parse(part.body);
		}
	}
	return part;
}

function nonEmpty(item) {
	return !!item;
}

function sortLastModifiedHeader(a, b) {
	return a.headers['last-modified-sort'] > b.headers['last-modified-sort'];
}

function parseMultiPartResponse (body, res) {
	if (body && res) {
		var boundary = utils.extractBoundary(contentType);
		var escapedBoundary = boundary.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&');
		var outerBoundary = new RegExp('\r?\n--'.concat(escapedBoundary, '--\r?\n'));
		var innerBoundary = new RegExp('\r?\n--'.concat(escapedBoundary, '\r?\n'));
		var parts = body.split(outerBoundary);
		parts = ((parts) !== null ? parts[0] : undefined) || "";
		parts = parts.split(innerBoundary).filter(nonEmpty)
		.map(utils.parseHttpHeadersAndBody).filter(nonEmpty)
		.map(utils.decodeMultipartPart);
		parts.sort(utils.sortLastModifiedHeader);

		res.body = parts;
	}
}

function appendPath(base, plus) {
	var end = base && base.length > 0 && base[base.length-1] === '/';
	var begin = plus && plus.length > 0 && plus[0] === '/';
	if (begin && end) { return base.concat(plus.substring(1)); }
	return (end || begin) ? base.concat(plus) : base.concat('/',plus);
}

function makeLink(accum, link) {
	// this is bound to an array when it gets called...
	var decoded, capture = link.trim().match(/^<\/([^\/]+)\/([^\/]+)\/([^\/]+)\/([^\/]+)>;\sriaktag="(.+)"$/);
	// new style link: <buckets/bucket/keys/key>; riaktag="tag"
	if (capture) {
		decoded = capture.map(decodeURIComponent);
		accum.push({
			kind: 'item',
			bucket: decoded[2],
			key: decoded[4],
			tag: decoded[5]
		});
	}
	else {
		// old style link: <riak/bucket/key>; riaktag="tag"
		capture = link.trim().match(/^<\/([^\/]+)\/([^\/]+)\/([^\/]+)>;\sriaktag="(.+)"$/);
		if (capture) {
			decoded = capture.map(decodeURIComponent);
			accum.push({
				kind: 'item',
				bucket: decoded[2],
				key: decoded[3],
				tag: decoded[4]
			});
		}
		else {
			// up link to bucket: <buckets/bucket>; rel="up"
			capture = link.trim().match(/^<\/([^\/]+)\/([^\/]+)>;\srel="(.+)"$/);
			if (capture) {
				decoded = capture.map(decodeURIComponent);
				accum.push({
					kind: 'bucket',
					bucket: decoded[2],
					tag: decoded[3]
				});
			}
			else {
				// unidentified link
				accum.push({
					kind: 'unidentified',
					raw: link
				});
			}
		}
	}
	return accum;
}

function makeLinks(links) {
	var result = [];
	if (links) {
		_.reduce(links.split(','), makeLink, result);
	}
	return result;
}

function linksToHeader(links) {
	return links.map(function(link) {
		// formulate all as new style
		switch (link.kind) {
			case 'item':
				return "</buckets/" + (encodeURIComponent(link.bucket))
				+ "/keys/" + (encodeURIComponent(link.key))
				+ ">; riaktag=\"" + (encodeURIComponent(link.tag || "_")) + "\"";
			case 'bucket':
				return "</buckets/" + (encodeURIComponent(link.bucket))
				+ ">; rel=\"" + (encodeURIComponent(link.tag || "_")) + "\"";
			case 'unidentified':
				return link.raw;
		}
	}).join(", ");
}

function prepareRequestOptions(uri, req) {
	var opt =	{
		uri: uri
	};
	if (req.options) {
		extend(opt, req.options);
	}
	return opt;
}

function prepareQueryPhase(name, opt) {
	var it = {};
	// default to javascript when no language given
	it.language = opt.language || 'javascript';
	if (typeof opt[name] === 'string') {
		it.name = opt[name];
	}
	else if (typeof opt[name] === 'function') {
		it.source = opt[name].toString();
	}
	if (typeof opt.arg !== 'undefined') {
		it.arg = opt.arg;
	}
	if (opt.keep) { it.keep = (opt.keep) ? true : false; }

	var phase = {};
	phase[name] = it;
	return phase;
}

function prepareQuery(input) {
	if (input.map) {
		return prepareQueryPhase('map', input);
	}
	if (input.reduce) {
		return prepareQueryPhase('reduce', input);
	}
	throw new Error('phases must define either a map or a reduce function');
}

defines(exports).enumerable
	.method(sequence)
	.method(appendPath)
	.method(padL)
	.method(extractBoundary)
	.method(filterHeaders)
	.method(makeLink)
	.method(makeLinks)
	.method(linksToHeader)
	.method(prepareRequestOptions)
	.method(prepareQueryPhase)
	.method(prepareQuery)
	;