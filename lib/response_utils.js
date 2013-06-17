"use strict";

function boundaryClense(text) {
	return (text) ? text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&") : text;
}

function filterNonEmpty(e) {
	return !!e;
}

function parseHttpHeadersAndBody(part) {
	var body, headers, parsed, res, md = part.split(/\r?\n\r?\n/);
	if (md){
		headers = md[0]; body = md[1];
		parsed = {};
		headers.split(/\r?\n/).forEach(function(header) {
			var ea = header.split(': '),
			k = ea[0],
			v = ea[1];
			return (parsed[k.toLowerCase()] = v);
		});
		return {
			header: parsed,
			body: body
		};
	}
}

function parseMultiPart(data, boundary) {
	var boundaryRegex = new RegExp("\r?\n--" + (boundaryClense(boundary)) + "--\r?\n");
	var parts = data.split(boundaryRegex);
	parts = ((parts) !== null ? parts[0] : undefined) || "";
	parts = parts.split(boundaryRegex).filter(filterNonEmpty);
	parts = parts.map(parseHttpHeadersAndBody).filter(filterNonEmpty);
	return parts;
}

function extractBoundary(header_string) {
	var c = header_string.match(/boundary=([A-Za-z0-9\'()+_,-.\/:=?]+)/);
	if (c) {
		return c[1];
	}
}

function decodeContent(data, contentType) {
	if (typeof contentType !== 'string') {
		throw new TypeError('[String] contentType is required');
	}
	var res;
	if (contentType.match(/^application\/binary/)) {
		res = new Buffer(data, 'binary');
	} else if (contentType.match(/^application\/octet-stream/)) {
		res = new Buffer(data, 'binary');
	} else if (contentType.match(/^application\/json/)) {
		res = JSON.parse(data);
	} else { res = data; }
	return res;
}

exports.parseMultipart  = parseMultiPart;
exports.extractBoundary = extractBoundary;
exports.decodeContent   = decodeContent;
