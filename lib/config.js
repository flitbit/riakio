var tmpl = require('url-template')
, svrTemplate = tmpl.parse('{scheme}://{host}:{port}')
, svrTemplateNP = tmpl.parse('{scheme}://{host}')
, config = {
	defaultScheme: 'http',
	defaultPort: 8098,
	servers: {
		default: {
			scheme: 'http',
			host: '127.0.0.1',
			port: 8098
		}
	}
}
, initialized
;

function $init($riakio_config) {
	var p
	;
	if (!initialized && $riakio_config) {
		config.defaultScheme = $riakio_config.defaultScheme || 'http';
		config.defaultPort = $riakio_config.defaultPort || 8098;
		var servers = $riakio_config.servers;
		if (servers) {
			for(p in servers) {
				config.servers[p] = {
					scheme: servers[p].scheme || config.defaultScheme,
					host: servers[p].host || '127.0.0.1',
					port: servers[p].port
				}
			}
		}
		initialized = true;
	}
}

Object.defineProperties($init, {

	serverFromOptions: {
		value: function(options) {
			if (options && options.server) {
				if (options.server.name) {
					return this.server(options.server.name);
				}
				return svrTemplate.expand({
					scheme: options.server.scheme || config.defaultScheme,
					host: options.server.host || '127.0.0.1',
					port: options.server.port
				})
			}
			else {
				return this.server('default');
			}
		},
		enumerable: true
	},

	server: {
		value: function(name) {
			var svr = config.servers[name];
			if (!svr) throw new Error('Server not configured: '.concat(name, '.'));
			return (svr.port) ? svrTemplate.expand(svr) : svrTemplateNP.expand(svr);
		},
		enumerable: true
	}

});

module.exports = $init;