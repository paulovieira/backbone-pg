var Backbone = require("backbone"),
	       _ = require('underscore'),
	       Q = require('q'),
	      pg = require('pg').native;

var changeCaseKeys = require('change-case-keys');


/*
The execute method will simply do this:
1) call Backbone.syncPostgres (overriden), passing the correct arguments; syncPostgres will return a Q promise
2) the promise is fulfilled with the data retrieved the query (using the pg module)
2) if the promise is fulfilled, parse the data (optional) and set/reset the collection;
3) if the promise is rejected, throw the error

Note: the .then method returns a new promise, which we retun to the caller
*/
Backbone.Collection.prototype.execute = function(options) {
	var promise, 
		options = options || {},
		collection = this,
		method = options.reset ? "reset" : "set";

	// change the case of the keys (or the objects in the result.rows array); pass false to disable;
	// should be one of: "camelize" (default), "underscored" or "dasherize" (underscore.string methods)
	options.changeCase = _.result(options, "changeCase");
	if(options.changeCase==="undefined"){ options.changeCase = _.result(collection, "changeCase"); }

	if(options.changeCase !== false){  
		options.changeCase = _.isString(options.changeCase) ? options.changeCase : "camelize";
	}

	promise = this
			.sync(this, options)
			.then(
				function(resp){
debugger;
					// allow the .parse method to be overriden directly at the options level
					// (this allows for customized parse function for the different usages - 
					// read/create/update/delete; note that the collection-level .parse method
					// will still be called at .set()/reset() (the default implementation is a noop )
					if(_.isFunction(options.parse)){ resp = options.parse(resp, options); }

					collection[method](resp, options);
					collection.trigger('sync', collection, resp, options);

					return resp;
				}, 
				function(err) {
debugger;
					collection.trigger('error', collection, err, options);
					throw err;
	    		}
			);

	return promise;
};

Backbone.Collection.prototype.sync = function() {
	return Backbone.syncPostgres.apply(this, arguments);
},


/***
Backbone.syncPostgres has a role similiar to Backbone.sync in the browser; it's where 
we use the pg module connect to the database, execute the command (options.query) and
update the collection with the output (either through .set() or .reset())
****/

Backbone.syncPostgres = function(entity, options) {
debugger;
	options = options || {};

	var propertyError = function(property) {
		throw new Error('A "' + property + '" property or function must be specified');
	};

	// Ensure that we have a connection string/object in the options or in the entity itself
	// (connection can be either given as a string/obj or as a function that returns a string/obj)
	options.connection =   _.result(options, "connection") 
						|| _.result(entity, 'connection') 
						|| propertyError('connection');

	// same for the query object
	options.query =    _.result(options, 'query') 
					|| _.result(entity, 'query') 
					|| propertyError('query');

	// if the query accepts only 1 parameter, allow it to be given directly in query.arguments
	// (instead of using an array with that single parameter)
	var command = options.query.command;
	if(command.indexOf("$1") >= 0 && command.indexOf("$2") < 0){
		if(!_.isArray(options.query.arguments) && !_.isUndefined(options.query.arguments)){
			options.query.arguments = [options.query.arguments]
		}
	}

	var deferred = Q.defer();
	options.promise = deferred.promise;

	pg.connect(options.connection, function(err, client, done) {
		if(err) {  deferred.reject(err); return;  }

		// currently we are using simple parameterized queries; later we should switch to Prepared Statements:
		// more info: https://github.com/brianc/node-postgres/wiki/Prepared-Statements
		client.query(options.query.command, options.query.arguments, function(err, result) {

			done();

			if(err) {  deferred.reject(err); return;  }

			// change the case of the keys
			if(options.changeCase){ 
				changeCaseKeys(result.rows, options.changeCase);
			}
			deferred.resolve(result.rows);

			if(options.disconnect){ pg.end(); }
		});
	});

	entity.trigger('request', entity, options.promise, options);
	return deferred.promise;
};


Backbone.Collection.prototype.disconnect = function() {
	pg.end();	
};

module.exports = Backbone;
