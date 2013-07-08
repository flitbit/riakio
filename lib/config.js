var tmpl = require('url-template')
, svrTemplate = tmpl.parse('{scheme}://{host}:{port}')
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

function $init($config) {
	var p
	;
	if (!initialized && $config) {
		config.defaultScheme = $config.get("riakio:defaultScheme") || 'http';
		config.defaultPort = $config.get("riakio:defaultPort") || 8098;
		var servers = $config.get("riakio:servers");
		if (servers) {
			for(p in servers) {
				config.servers[p] = {
					scheme: servers[p].scheme || config.defaultScheme,
					host: servers[p].host || '127.0.0.1',
					port: servers[p].port || config.defaultPort
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
					port: options.server.port || config.defaultPort
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
			if (!config.servers[name]) throw new Error('Server not configured: '.concat(name, '.'));
			return svrTemplate.expand(config.servers[name]);
		},
		enumerable: true
	}

});

module.exports = $init;